import redis, rpc, logging
import messages_pb2 as msg

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

logging.basicConfig(level=logging.DEBUG)

server=ChunkServer()
sched=rpc.Scheduler()
guid=msg.Guid()
guid.a=9; guid.b=8; guid.c=7; guid.d=6;
service=rpc.RpcServiceCo(sched, server)
service.listen(guid)

