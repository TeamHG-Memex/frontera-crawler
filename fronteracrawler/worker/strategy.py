# -*- coding: utf-8 -*-
from fronteracrawler.strategies import topic
from jsonrpc_service import StrategyWorkerWebService
from zookeeper import ZookeeperSession

import logging
from argparse import ArgumentParser
from urllib import urlopen
from json import loads

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
    def __init__(self, log_processing):
        self.log_processing = CallLaterOnce(log_processing)
        self.log_processing.setErrback(self.error)
        self.scheduling = CallLaterOnce(self.schedule)
        self.scheduling.setErrback(self.error)
        self.is_active = False

    def error(self, f):
        logger.error(f)

    def schedule(self):
        if self.is_active:
            self.log_processing.schedule()

        self.scheduling.schedule(1.0)


class HHStrategyWorker(ScoringWorker):
    def __init__(self, settings):
        super(HHStrategyWorker, self).__init__(settings, topic)
        self.slot = Slot(log_processing=self.work)
        kafka_hh = KafkaClient(settings.get('KAFKA_LOCATION_HH'))
        self.consumer_hh = SimpleConsumer(kafka_hh,
                                          settings.get('FRONTERA_GROUP'),
                                          settings.get('FRONTERA_INCOMING_TOPIC'),
                                          buffer_size=262144,
                                          max_buffer_size=10485760)
        self.producer_hh = SimpleProducer(kafka_hh)
        self.results_topic = settings.get("FRONTERA_RESULTS_TOPIC")
        self.job_config = {}
        self.zookeeper = ZookeeperSession(settings.get('ZOOKEEPER_LOCATION'), worker_prefix='hh-strategy-worker')

    def set_process_info(self, process_info):
        self.process_info = process_info
        self.zookeeper.set(process_info)

    def run(self):
        self.slot.schedule()
        reactor.run()

    def incoming(self):
        consumed = 0
        try:
            for m in self._in_consumer.get_messages(count=32):
                try:
                    msg = loads(m.message.value)
                except ValueError, ve:
                    logger.error("Decoding error %s, message %s" % (ve, m.message.value))
                else:
                    self.job_config = {
                        'workspace': msg['workspace'],
                        'nResults': msg.get('nResults', 0),
                        'timestamp': msg['timestamp'],
                        'source': msg['source'],
                        'excluded': msg['excluded'],
                        'included': msg['included']
                    }
                finally:
                    consumed += 1
        except OffsetOutOfRangeError, e:
            # https://github.com/mumrah/kafka-python/issues/263
            self._in_consumer.seek(0, 2)  # moving to the tail of the log
            logger.info("Caught OffsetOutOfRangeError, moving to the tail of the log.")

        self.stats['frontera_incoming_consumed'] = consumed
        self.slot.schedule()

    def setup(self, seed_urls):
        # Consume configuration from Kafka topic
        self.incoming()

        # TODO: That should be executed on all strategy worker instances
        self.configure(self.job_config)

        # Sending seed urls into pipeline
        requests = [self._manager.request_model(url) for url in seed_urls]
        self.send_add_seeds(requests)

        self.slot.is_active = True

    def reset(self):
        self.slot.is_active = False
        # switch job_id ("scoring" should be always in the same process as batch gen/log proc)
        # notify other workers via jsonrpc with new_job_id
        # if all is good:
        # change job_id in hbase backend: metadata, queue
        # make it active if all above is successful.

        self.job_id = generate_job_id()
        for location in self.zookeeper.get_workers():
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
        self.strategy.cnofigure(config)

    def send_add_seeds(self, seeds):
        # here we simulate behavior of spider, mainly because of middlewares pipeline
        settings = Settings(self._manager.settings)
        settings.set('BACKEND', KafkaBackend)
        settings.set('SPIDER_PARTITION_ID', 0)
        manager = FrontierManager.from_settings(settings)
        manager.add_seeds(seeds)
        del manager


if __name__ == '__main__':
    parser = ArgumentParser(description="HH strategy worker.")
    parser.add_argument('--config', type=str, required=True,
                        help='Settings module name, should be accessible by import')
    parser.add_argument('--log-level', '-L', type=str, default='INFO',
                        help="Log level, for ex. DEBUG, INFO, WARN, ERROR, FATAL")

    args = parser.parse_args()
    logger.setLevel(args.log_level)
    settings = Settings(module=args.config)
    worker = HHStrategyWorker(settings)
    web_service = StrategyWorkerWebService(worker, settings)
    web_service.start_listening()
    worker.run()
