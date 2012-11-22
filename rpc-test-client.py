import redis, rpc, logging
import messages_pb2 as msg
import mds_mock

logging.basicConfig(level=logging.DEBUG)

guid=msg.Guid()
guid.a=10
guid.b=22
guid.c=30
guid.d=40
stub=rpc.RpcStub(guid, mds_mock.MDS)

arg=msg.NewChunk_Request()
arg.size=32
arg.location.a=0
arg.location.b=0
arg.location.c=0
arg.location.d=0
ret=stub.callMethod('NewChunk', arg)
print ret

arg=msg.DeleteChunk_Request()
arg.guid.a=10
arg.guid.b=10
arg.guid.c=10
arg.guid.d=10
ret=stub.callMethod('DeleteChunk', arg)
print ret

