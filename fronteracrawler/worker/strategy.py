# -*- coding: utf-8 -*-
from fronteracrawler.strategies import topic
from jsonrpc_service import StrategyWorkerWebService
from zookeeper import ZookeeperSession

import logging
from argparse import ArgumentParser
from urllib import urlopen
from json import loads, dumps
from itertools import islice

from crawlfrontier.settings import Settings
from crawlfrontier.worker.score import ScoringWorker
from crawlfrontier.worker.utils import CallLaterOnce
from crawlfrontier.core.manager import FrontierManager
from crawlfrontier.contrib.backends.remote import KafkaBackend
from crawlfrontier.utils.misc import generate_job_id
from twisted.internet import reactor
from kafka import KafkaClient, SimpleConsumer, SimpleProducer
from kafka.common import OffsetOutOfRangeError


logging.basicConfig()
logger = logging.getLogger("score")


class Slot(object):
    def __init__(self, log_processing, incoming, outgoing, is_master):
        self.log_processing = CallLaterOnce(log_processing)
        self.log_processing.setErrback(self.error)
        self.incoming = CallLaterOnce(incoming)
        self.incoming.setErrback(self.error)
        self.scheduling = CallLaterOnce(self.schedule)
        self.scheduling.setErrback(self.error)
        self.outgoing = CallLaterOnce(outgoing)
        self.outgoing.setErrback(self.error)
        self.is_active = False
        self.is_master = is_master

    def error(self, f):
        logger.error(f)

    def schedule(self):
        if self.is_active:
            self.log_processing.schedule()
        elif self.is_master:
            self.incoming.schedule()
        self.outgoing.schedule()
        self.scheduling.schedule(1.0)


class HHStrategyWorker(ScoringWorker):

    worker_prefix = 'hh-strategy-worker'

    def __init__(self, settings):
        super(HHStrategyWorker, self).__init__(settings, topic)
        self.slot = Slot(log_processing=self.work, incoming=self.incoming, outgoing=self.outgoing,
                         is_master=settings.get("FRONTERA_MASTER"))
        kafka_hh = KafkaClient(settings.get('KAFKA_LOCATION_HH'))
        self.consumer_hh = SimpleConsumer(kafka_hh,
                                          settings.get('FRONTERA_GROUP'),
                                          settings.get('FRONTERA_INCOMING_TOPIC'),
                                          buffer_size=262144,
                                          max_buffer_size=10485760)
        self.producer_hh = SimpleProducer(kafka_hh)
        self.results_topic = settings.get("FRONTERA_RESULTS_TOPIC")
        self.job_config = {}
        self.zookeeper = ZookeeperSession(settings.get('ZOOKEEPER_LOCATION'), name_prefix=self.worker_prefix)

    def set_process_info(self, process_info):
        self.process_info = process_info
        self.zookeeper.set(process_info)

    def run(self):
        self.slot.schedule()
        reactor.run()

    def incoming(self):
        if self.slot.is_active:
            return

        if not self.slot.is_master:
            logger.warn("Incoming topic shouldn't be consumed on slave instances.")

        consumed = 0
        try:
            for m in self.consumer_hh.get_messages(count=1):
                try:
                    msg = loads(m.message.value)
                except ValueError, ve:
                    logger.error("Decoding error %s, message %s" % (ve, m.message.value))
                else:
                    logger.info("Got incoming message %s from incoming topic." % m.message.value)
                    self.job_config = {
                        'workspace': msg['workspace'],
                        'nResults': msg.get('nResults', 0),
                        'excluded': msg['excluded'],
                        'included': msg['included'],
                        'relevantUrl': msg['relevantUrl'],
                        'irrelevantUrl': msg['irrelevantUrl'],
                    }
                    self.reset()
                    self.setup(self.job_config['relevantUrl'], self.job_config)
                finally:
                    consumed += 1
        except OffsetOutOfRangeError, e:
            # https://github.com/mumrah/kafka-python/issues/263
            self.consumer_hh.seek(0, 0)  # moving to the tail of the log
            logger.info("HH incoming topic, caught OffsetOutOfRangeError, moving to the tail of the log.")
        self.stats['frontera_incoming_consumed'] = consumed

    def outgoing(self):
        produced = 0
        if not self.strategy.results:
            return
        items = list(islice(self.strategy.results.iteritems(), 50))
        for fprint, result in items:
            msg = {
                "score": result[0],
                "url": result[1],
                "title": result[2],
                "descr": result[3],
                "keywords": result[4],
                "workspace": self.job_config.get('workspace', None),
                "provider": "Frontera"
            }
            self.producer_hh.send_messages(self.results_topic, dumps(msg))
            del self.strategy.results[fprint]
            produced += 1
        self.strategy.results.clear()
        self.stats['frontera_outgoing_produced'] = produced
        if produced > 0:
            logger.info("Wrote %d results to output topic." % produced)

    def setup(self, seed_urls, job_config):
        # Consume configuration from Kafka topic
        if not job_config:
            raise AttributeError('Expecting for job_config to be set.')
        else:
            self.job_config = job_config

        # Sending configuration to all instances
        locations = list(self.zookeeper.get_workers(prefix=self.worker_prefix)) + \
                    list(self.zookeeper.get_workers(prefix='spider'))
        for location in locations:
            if location == self.process_info:
                continue
            url = "http://%s/jsonrpc" % location
            reqid = generate_job_id()
            data = '{"id": %d, "method": "configure", "params": %s}' % (reqid, dumps(self.job_config))
            response = urlopen(url, data).read()
            result = loads(response)
            assert result['id'] == reqid
            if 'result' not in result or result['result'] != "success":
                logger.error("Can't configure %s, error %s" % (location, result['error']))
                raise Exception("Error configuring")
        self.configure(self.job_config)

        # Sending seed urls into pipeline
        requests = [self._manager.request_model(url, meta={'jid': self.job_id}) for url in seed_urls]
        self.send_add_seeds(requests)

    def reset(self):
        self.slot.is_active = False
        # switch job_id ("scoring" should be always in the same process as batch gen/log proc)
        # notify other workers via jsonrpc with new_job_id
        # if all is good:
        # change job_id in hbase backend: metadata, queue
        # make it active if all above is successful.

        self.job_id = generate_job_id()
        for location in self.zookeeper.get_workers(exclude_prefix='spider'):
            if location == self.process_info:
                continue
            url = "http://%s/jsonrpc" % location
            reqid = generate_job_id()
            data = '{"id": %d, "method": "new_job_id", "job_id": %d}' % (reqid, self.job_id)
            response = urlopen(url, data).read()
            result = loads(response)
            assert result['id'] == reqid
            if 'result' not in result or result['result'] != "success":
                logger.error("Can't set new job id on %s, error %s" % (location, result['error']))
                raise Exception("Error setting new job id")
        self.backend.set_job_id(self.job_id)

    def configure(self, config):
        logger.info("Got configure call, config is %s" % (str(config)))
        self.strategy.configure(config)
        self.slot.is_active = True

    def set_job_id(self, job_id):
        self.job_id = job_id
        self.backend.set_job_id(self.job_id)

    def send_add_seeds(self, seeds):
        # here we simulate behavior of spider, mainly because of middlewares pipeline
        settings = Settings(self._manager.settings)
        settings.set('BACKEND', KafkaBackend)
        settings.set('SPIDER_PARTITION_ID', 0)
        manager = FrontierManager.from_settings(settings)
        manager.start()
        manager.add_seeds(seeds)
        del manager

    def on_finished(self):
        self.slot.is_active = False


if __name__ == '__main__':
    parser = ArgumentParser(description="HH strategy worker.")
    parser.add_argument('--config', type=str, required=True,
                        help='Settings module name, should be accessible by import')
    parser.add_argument('--log-level', '-L', type=str, default='INFO',
                        help="Log level, for ex. DEBUG, INFO, WARN, ERROR, FATAL")
    parser.add_argument('--master', '-m', action='store_true')

    args = parser.parse_args()
    logger.setLevel(args.log_level)
    settings = Settings(module=args.config)
    settings.set("FRONTERA_MASTER", args.master)
    if args.master:
        logger.info("Configured as MASTER")
    worker = HHStrategyWorker(settings)
    web_service = StrategyWorkerWebService(worker, settings)
    web_service.start_listening()
    worker.run()
