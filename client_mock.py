import rpc, logging
import messages_pb2 as msg
import ChunkServer
import gevent.socket
import mds

MDS_IP='192.168.0.149'
MDS_PORT=2345
CHK_IP='192.168.0.149'
CHK_PORT=6789

logging.basicConfig(level=logging.DEBUG)
socket=gevent.socket.socket()
socket.connect((MDS_IP, MDS_PORT))

guid=msg.Guid()
guid.a=10
guid.b=22
guid.c=30
guid.d=40
stub=rpc.RpcStub(guid, socket, mds.MDS)

arg=msg.GetChunkServers_Request()
# arg.randomCount=3
ret=stub.callMethod('GetChunkServers', arg)
print ret.random[0]
# socket.close()

s2=gevent.socket.socket()
s2.connect((ret.random[0].ServiceAddress, ret.random[0].ServicePort))
stub2=rpc.RpcStub(guid, s2, ChunkServer.ChunkServer)
arg2=msg.NewChunk_Request()
arg2.size=32
arg2.count=2
ret2=stub2.callMethod('NewChunk', arg2)

# guids = ret2.guids
# stub2.callMethod('AssembleVolume', arg_assemble_request)



