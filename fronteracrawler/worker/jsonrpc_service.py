# -*- coding: utf-8 -*-
from crawlfrontier.worker.server import JsonRpcService, JsonResource, WorkerJsonRpcResource, RootResource, \
    JsonRpcError, jsonrpc_result


class StatusResource(JsonResource):

    ws_name = 'status'

    def __init__(self, worker):
        self.worker = worker
        JsonResource.__init__(self)

    def render_GET(self, txrequest):
        return {
            'stats': self.worker.stats,
            'job_id': self.worker.job_id
        }


class ManagementResource(WorkerJsonRpcResource):

    ws_name = 'jsonrpc'

    def process_request(self, method, jrequest):
        # master methods
        if method == 'setup':
            self.worker.setup(jrequest['params']['seed_urls'], jrequest['params'].get('job_config', None))
            return jsonrpc_result(jrequest['id'], "success")
        if method == 'reset':
            self.worker.reset()
            return jsonrpc_result(jrequest['id'], {"status": "success", "job_id": self.worker.job_id})

        # slave
        if method == 'configure':
            if type(jrequest['params']) != dict:
                raise JsonRpcError(400, "Expecting dict with configuration parameters.")
            self.worker.configure(jrequest['params'])
            return jsonrpc_result(jrequest['id'], "success")
        if method == 'new_job_id':
            if type(jrequest['job_id']) != int or jrequest['job_id'] <= 0:
                raise JsonRpcError(400, "Job id must be of type unsigned integer, bigger than 0")
            self.worker.set_job_id(jrequest['job_id'])
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


class FronteraWorkerResource(WorkerJsonRpcResource):

    ws_name = 'jsonrpc'

    def process_request(self, method, jrequest):
        if method == 'new_job_id':
            self.worker.set_job_id(jrequest['job_id'])
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