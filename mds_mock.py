import rpc, logging, gevent.socket
import messages_pb2 as msg
import guid as Guid
import random
import ChkSvr_mock

def myNewChunk(arg):
	ret=msg.NewChunk_Response()
	ret.size=arg.size
	ret.guid.a=0x66
	ret.guid.b=0x77
	ret.guid.c=0x88
	ret.guid.d=0x99
	return ret

class MDS:
	def __init__(self):
		self.registry={}
		self.service=None
		self.guid=Guid.generate()
		self.stub=rpc.RpcStub(self.guid, None, ChkSvr_mock.ChunkServer)		
	def _onConnectionClose_(self):
		guid=self.service.peerGuid()
		key=Guid.toTuple(guid)
		if key in self.registry:
			del self.registry[key]
	def ChunkServerInfo(self, arg):
		logging.debug(type(arg))
		guid=self.service.peerGuid()
		key=Guid.toTuple(guid)
		self.registry[key]=arg
	def NewChunk(self, arg):
		print type(arg)
		guids=self.registry.keys()
		x=random.choice(guids)
		print "Relay NewChunk to ", x
		ckinfo=self.registry[x]
		socket=gevent.socket.socket()
		socket.connect((ckinfo.ServiceAddress, ckinfo.ServicePort))
		try:
			ret=self.stub.callMethod("NewChunk", arg, socket)
		finally:
			socket.close()
		print "Got response: ", ret
		return ret
	def DeleteChunk(self, arg):
		print type(arg)
		ret=msg.DeleteChunk_Response()
		for x in arg.guids:
			ret.guids.add()
			y=ret.guids[-1]
			Guid.assign(y, x)
		return ret
	def NewVolume(self, arg):
		print type(arg)
		ret=msg.NewVolume_Response()
		return ret
	def AssembleVolume(self, arg):
		print type(arg)
		ret=msg.AssembleVolume_Response()
		ret.access_point='no access_point'
		return ret
	def DisassembleVolume(self, arg):
		print type(arg)
		ret=msg.DisassembleVolume_Response()
		ret.access_point=arg.access_point
		return ret
	def RepairVolume(self, arg):
		print type(arg)
		ret=msg.RepairVolume_Response()
		return ret
	def ReadVolume(self, arg):
		print type(arg)
		ret=msg.ReadVolume_Response()
		ret.volume.size=0
		return ret
	def WriteVolume(self, arg):
		print type(arg)
		ret=msg.WriteVolume_Response()
		return ret
	def MoveVolume(self, arg):
		print type(arg)
		ret=msg.MoveVolume_Response()
		return ret
	def ChMod(self, arg):
		print type(arg)
		ret=msg.ChMod_Response()
		return ret
	def DeleteVolume(self, arg):
		print type(arg)
		ret=msg.DeleteVolume_Response()
		return ret
	def CreateLink(self, arg):
		print type(arg)
		ret=msg.CreateLink_Response()
		return ret


