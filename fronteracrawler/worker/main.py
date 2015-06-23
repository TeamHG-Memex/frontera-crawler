# -*- coding: utf-8 -*-

from argparse import ArgumentParser
import logging

from crawlfrontier.settings import Settings
from crawlfrontier.worker.main import FrontierWorker
from kazoo.client import KazooClient, KazooState

from fronteracrawler.worker.jsonrpc_service import FronteraWorkerWebService


logging.basicConfig()
logger = logging.getLogger("cf")

class HHFrontierWorker(FrontierWorker):
    def __init__(self, settings, no_batches, no_scoring, no_incoming):
        super(HHFrontierWorker, self).__init__(settings, no_batches, no_scoring, no_incoming)
        self.init_zookeeper()

    def init_zookeeper(self):
        self._zk = KazooClient(hosts=settings.get('ZOOKEEPER_LOCATION'))
        self._zk.add_listener(self.zookeeper_listener)
        self._zk.start()
        self.znode_path = self._zk.create("/frontera/hh-f-worker", ephemeral=True, sequence=True, makepath=True)

    def zookeeper_listener(self, state):
        if state == KazooState.LOST:
            # Register somewhere that the session was lost
            pass
        elif state == KazooState.SUSPENDED:
            # Handle being disconnected from Zookeeper
            pass
        else:
            # Handle being connected/reconnected to Zookeeper
            pass

    def set_process_info(self, process_info):
        self.process_info = process_info
        self._zk.set(self.znode_path, self.process_info)


if __name__ == '__main__':
    parser = ArgumentParser(description="Crawl frontier worker.")
    parser.add_argument('--no-batches', action='store_true',
                        help='Disables periodical generation of new batches')
    parser.add_argument('--no-scoring', action='store_true',
                        help='Disables periodical consumption of scoring topic')
    parser.add_argument('--no-incoming', action='store_true',
                        help='Disables periodical incoming topic consumption')
    parser.add_argument('--config', type=str, required=True,
                        help='Settings module name, should be accessible by import')
    parser.add_argument('--log-level', '-L', type=str, default='INFO',
                        help="Log level, for ex. DEBUG, INFO, WARN, ERROR, FATAL")
    parser.add_argument('--port', type=int, help="Json Rpc service port to listen")
    args = parser.parse_args()
    logger.setLevel(args.log_level)
    settings = Settings(module=args.config)
    if args.port:
        settings.set("JSONRPC_PORT", [args.port])

    worker = HHFrontierWorker(settings, args.no_batches, args.no_scoring, args.no_incoming)
    server = FronteraWorkerWebService(worker, settings)
    server.start_listening()
    worker.run()