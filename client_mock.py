import rpc, logging
import messages_pb2 as msg
import ChunkServer
import gevent.socket
import mds
import guid as Guid

MDS_IP='192.168.0.149'
MDS_PORT=2340
CHK_IP='192.168.0.149'
CHK_PORT=67802

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
ret=stub.callMethod('GetChunkServers', arg)
print ret.random[0]
# socket.close()

s2=gevent.socket.socket()
s2.connect((ret.random[0].ServiceAddress, ret.random[0].ServicePort))
stub2=rpc.RpcStub(guid, s2, ChunkServer.ChunkServer)

if raw_input('you want to NewChunk ? ')=='y':
	arg2=msg.NewChunk_Request()
	arg2.size=32
	arg2.count=2
	ret2=stub2.callMethod('NewChunk', arg2)
	print ret2.guids

if raw_input('you want to assembleVolume ? ')=='y':
	req_assemble = msg.AssembleVolume_Request()
	req_assemble.volume.size = 32
	Guid.assign(req_assemble.volume.guid,ret2.guids[-1])
	res_assemble = stub2.callMethod('AssembleVolume', req_assemble)
	print 'access_point:', res_assemble.access_point

if raw_input('you want to DisassembleVolume ? ')=='y':
	req_disassemble = msg.DisassembleVolume_Request()
	req_disassemble.access_point = res_assemble.access_point
	print 'req_disassemble.access_point:', req_disassemble.access_point
	stub2.callMethod('DisassembleVolume', req_disassemble)

if raw_input('you want to DeleteChunk ? ')=='y':
	arg3=msg.DeleteChunk_Request()
	t = arg3.guids.add()
	Guid.assign(t, ret2.guids[0])
	t = arg3.guids.add()
	Guid.assign(t, ret2.guids[1])
	ret_Del=stub2.callMethod('DeleteChunk', arg3)
	

