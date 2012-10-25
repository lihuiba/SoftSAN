import redis, rpc
import messages_pb2 as msg
import mds_mock

server=mds_mock.MDS()
service=rpc.RpcService(server)
while True:
	print 1
	service.listen()

