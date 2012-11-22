import redis, rpc, logging
import messages_pb2 as msg
import mds_mock


logging.basicConfig(level=logging.DEBUG)


sched=rpc.Scheduler()
stub=rpc.RpcStubCo(msg.Guid(), sched, mds_mock.MDS)
server=mds_mock.MDS(stub)
service=rpc.RpcServiceCo(sched, server)
stub.setServiceCo(service)
while True:
	print 1
	service.listen()

