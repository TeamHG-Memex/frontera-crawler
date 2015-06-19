# -*- coding: utf-8 -*-
from strategies import topic
from jsonrpc_service import ManagementWebService

from crawlfrontier.settings import Settings
from crawlfrontier.worker.score import ScoringWorker
from crawlfrontier.worker.utils import CallLaterOnce
from twisted.internet import reactor
from kafka import KafkaClient, SimpleConsumer, SimpleProducer
from kafka.common import OffsetOutOfRangeError

import logging
from argparse import ArgumentParser


logging.basicConfig()
logger = logging.getLogger("score")


class Slot(object):
    def __init__(self, log_processing, consume_incoming):
        self.log_processing = CallLaterOnce(log_processing)
        self.log_processing.setErrback(self.error)
        self.consume_incoming = CallLaterOnce(consume_incoming)
        self.consume_incoming.setErrback(self.error)
        self.scheduling = CallLaterOnce(self.schedule)
        self.scheduling.setErrback(self.error)
        self.is_active = False

    def error(self, f):
        logger.error(f)
        reactor.stop()

    def schedule(self):
        self.log_processing.schedule()
        if self.is_active:
            self.consume_incoming.schedule()

        self.scheduling.schedule(1.0)


class HHStrategyWorker(ScoringWorker):
    def __init__(self, settings):
        super(HHStrategyWorker, self).__init__(settings, topic)
        self.slot = Slot(log_processing=self.work, consume_incoming=self.incoming)
        kafka_hh = KafkaClient(settings.get('KAFKA_LOCATION_HH'))
        self.consumer_hh = SimpleConsumer(kafka_hh,
                                          settings.get('FRONTERA_GROUP'),
                                          settings.get('FRONTERA_INCOMING_TOPIC'),
                                          buffer_size=262144,
                                          max_buffer_size=10485760)
        self.producer_hh = SimpleProducer(kafka_hh)
        self.results_topic = settings.get("FRONTERA_RESULTS_TOPIC")

    def run(self):
        self.slot.schedule()
        reactor.run()

    def incoming(self):
        consumed = 0
        try:
            for m in self._in_consumer.get_messages(count=32):
                consumed += 1
        except OffsetOutOfRangeError, e:
            # https://github.com/mumrah/kafka-python/issues/263
            self._in_consumer.seek(0, 2)  # moving to the tail of the log
            logger.info("Caught OffsetOutOfRangeError, moving to the tail of the log.")

        self.stats['frontera_incoming_consumed'] = consumed
        self.slot.schedule()

    def setup(self, seed_urls):
        pass

    def reset(self):
        self.slot.is_active = False


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
    mgmt_service = ManagementWebService(worker, settings)
    mgmt_service.start_listening()
    worker.run()
