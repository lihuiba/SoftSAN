import rpc, logging
import messages_pb2 as msg
import mds, ChkSvr_mock
import gevent.socket


logging.basicConfig(level=logging.DEBUG)

socket=gevent.socket.socket()
socket.connect(('localhost', 2345))

guid=msg.Guid()
guid.a=10
guid.b=22
guid.c=30
guid.d=40
stub=rpc.RpcStub(guid, socket, mds.MDS)

arg=msg.GetChunkServers_Request()
arg.randomCount=3
ret=stub.callMethod('GetChunkServers', arg)
print ret

s2=gevent.socket.socket()
x=ret.random[0]
s2.connect((x.ServiceAddress, x.ServicePort))
stub2=rpc.RpcStub(guid, s2, ChkSvr_mock.ChunkServer)
arg=msg.NewChunk_Request()
arg.size=32
arg.count=1
ret=stub2.callMethod('NewChunk', arg)
print ret