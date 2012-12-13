import rpc, logging
import messages_pb2 as msg
import guid as Guid
import mds_mock
import gevent.server

PORT=6789

def heartBeat():
	global guid
	info=msg.ChunkServerInfo()
	info.ServiceAddress='localhost'
	info.ServicePort=PORT
	info.chunks.add()
	c=info.chunks[0]
	c.guid.a=0x66
	c.guid.b=0x77
	c.guid.c=0x88
	c.guid.d=0x99
	c.size=64
	socket=gevent.socket.socket()
	socket.connect(('localhost', 2345))
	stub=rpc.RpcStub(guid, socket, mds_mock.MDS)
	while True:
		print 'calling ChunkServerInfo'
		stub.callMethod('ChunkServerInfo', info)
		gevent.sleep(3)

class ChunkServer:
	def NewChunk(self, req):
		print req
		ret=msg.NewChunk_Response()
		ret.size=req.size
		for i in range(req.count):
			ret.guids.add()
			Guid.generate(ret.guids[-1])
		return ret

if __name__=='__main__':
	guid=msg.Guid()
	guid.a=9; guid.b=8; guid.c=7; guid.d=6;
	logging.basicConfig(level=logging.DEBUG)

	gevent.spawn(heartBeat)

	server=ChunkServer()
	service=rpc.RpcService(server)
	framework=gevent.server.StreamServer(('0.0.0.0', PORT), service.handler)
	framework.serve_forever()

