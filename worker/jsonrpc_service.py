# -*- coding: utf-8 -*-
from crawlfrontier.worker.server import JsonRpcService, JsonResource, JsonRpcResource, RootResource, \
    JsonRpcError, jsonrpc_result


class StatusResource(JsonResource):

    ws_name = 'status'

    def __init__(self, worker):
        self.worker = worker
        JsonResource.__init__(self)

    def render_GET(self, txrequest):
        return {
            'is_active': self.worker.slot.is_active,
            'stats': self.worker.stats
        }


class ManagementResource(JsonRpcResource):

    ws_name = 'jsonrpc'

    def process_request(self, method, jrequest):
        if method == '/setup':
            self.worker.setup(jrequest['seed_urls'])
            return jsonrpc_result(jrequest['id'], "success")

        if method == '/reset':
            self.worker.reset()
            return jsonrpc_result(jrequest['id'], "success")
        raise JsonRpcError(400, "Unknown method")


class StrategyWorkerWebService(JsonRpcService):

    def __init__(self, worker, settings):
        root = RootResource()
        root.putChild('status', StatusResource(worker))
        root.putChild('jsonrpc', ManagementResource(worker))
        JsonRpcService.__init__(self, root, settings)
        self.worker = worker

    def start_listening(self):
        JsonRpcService.start_listening(self)
        address = self.port.getHost()
        self.worker.set_process_info("%s:%d" % (address.host, address.port))


class FronteraWorkerResource(JsonRpcResource):

    ws_name = 'jsonrpc'

    def process_request(self, method, jrequest):
        if method == '/new_job_id':
            self.worker.job_id = jrequest['job_id']
            return jsonrpc_result(jrequest['id'], "success")
        raise JsonRpcError(400, "Unknown method")


class FronteraWorkerWebService(JsonRpcService):
    def __init__(self, worker, settings):
        root = RootResource()
        root.putChild('status', StatusResource(worker))
        root.putChild('jsonrpc', FronteraWorkerResource(worker))
        JsonRpcService.__init__(self, root, settings)
        self.worker = worker

    def start_listening(self):
        JsonRpcService.start_listening(self)
        address = self.port.getHost()
        self.worker.set_process_info("%s:%d" % (address.host, address.port))