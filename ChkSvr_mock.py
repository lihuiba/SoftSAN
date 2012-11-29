import redis, rpc, logging
import messages_pb2 as msg
import mds_mock, mds

class ChunkServer:
	def NewChunk(self, req):
		print req
		ret=msg.NewChunk_Response()
		ret.size=req.size
		ret.guid.a=0x66
		ret.guid.b=0x77
		ret.guid.c=0x88
		ret.guid.d=0x99
		return ret


guid=msg.Guid()
guid.a=9; guid.b=8; guid.c=7; guid.d=6;
logging.basicConfig(level=logging.DEBUG)

info=msg.ChunkServerInfo()
info.guid.a=guid.a
info.guid.b=guid.b
info.guid.c=guid.c
info.guid.d=guid.d
info.chunks.add()
c=info.chunks[0]
c.guid.a=0x66
c.guid.b=0x77
c.guid.c=0x88
c.guid.d=0x99
c.size=64
print 2341234
stub=rpc.RpcStub(guid, mds_mock.MDS)
stub.callMethod('ChunkServerInfo', info)

#server=ChunkServer()
#sched=rpc.Scheduler()
#service=rpc.RpcServiceCo(sched, server)
#service.listen(guid)

