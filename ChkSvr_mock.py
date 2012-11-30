import redis, rpc, logging
import messages_pb2 as msg
import mds_mock, mds
import timer, time

class HeartBeater:
	def __init__(self, guid):
		self.info=msg.ChunkServerInfo()
		self.info.guid.a=guid.a
		self.info.guid.b=guid.b
		self.info.guid.c=guid.c
		self.info.guid.d=guid.d
		self.info.chunks.add()
		c=self.info.chunks[0]
		c.guid.a=0x66
		c.guid.b=0x77
		c.guid.c=0x88
		c.guid.d=0x99
		c.size=64
		self.timer=timer.Timer(0, self.init2)
	def init2(self):
		self.stub=rpc.RpcStub(guid, mds_mock.MDS)
		self.onTimer()
		self.timer.delegate(2, self.onTimer)
	def onTimer(self):
		print 'Heart beat'
		self.stub.callMethod('ChunkServerInfo', self.info)

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

hb=HeartBeater(guid)

# while True:
# 	time.sleep(10)
server=ChunkServer()
service=rpc.RpcService(server)
service.listen(guid)

