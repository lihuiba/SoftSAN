import rpc, logging
import messages_pb2 as msg
import mds_mock
import gevent.socket

logging.basicConfig(level=logging.DEBUG)

socket=gevent.socket.socket()
socket.connect(('localhost', 2345))


guid=msg.Guid()
guid.a=10
guid.b=22
guid.c=30
guid.d=40
stub=rpc.RpcStub(guid, socket, mds_mock.MDS)

arg=msg.NewChunk_Request()
arg.size=32
arg.location.a=0
arg.location.b=0
arg.location.c=0
arg.location.d=0
ret=stub.callMethod('NewChunk', arg)
print ret

arg=msg.DeleteChunk_Request()
arg.guids.add()
x=arg.guids[0]
x.a=10
x.b=10
x.c=10
x.d=10
ret=stub.callMethod('DeleteChunk', arg)
print ret

