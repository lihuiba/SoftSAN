import redis, rpc
import messages_pb2 as msg
import mds_mock

server=mds_mock.MDS()
guid=msg.Guid()
guid.a=10
guid.b=22
guid.c=30
guid.d=40
stub=rpc.RpcStub(server, guid)
arg=msg.NewChunk_Request()
arg.size=32
ret=stub.callMethod('NewChunk', arg)
print ret
