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

        if method == '/reset':
            self.worker.reset()
            return jsonrpc_result(jrequest['id'], "success")
        raise JsonRpcError(400, "Unknown method")


class ManagementWebService(JsonRpcService):

    def __init__(self, worker, settings):
        root = RootResource()
        root.putChild('status', StatusResource(worker))
        root.putChild('jsonrpc', JsonRpcResource(worker))
        JsonRpcService.__init__(self, root, settings)