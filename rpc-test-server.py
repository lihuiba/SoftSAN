import logging, gevent.server
import rpc, mds_mock

logging.basicConfig(level=logging.DEBUG)

server=mds_mock.MDS()
service=rpc.RpcService(server)


framework=gevent.server.StreamServer(('0.0.0.0', 2345), service.handler)
framework.serve_forever()