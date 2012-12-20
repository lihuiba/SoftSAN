import logging, inspect, sys, traceback
import messages_pb2 as msg
import guid as Guid

# MethodInfo={
#	"NewChunk":         (msg.NewChunk_Request,          msg.NewChunk_Response),
#	"DeleteChunk":      (msg.DeleteChunk_Request,       msg.DeleteChunk_Response),
#	"NewVolume":        (msg.NewVolume_Request,         msg.NewVolume_Response),
#	"AssembleVolume":   (msg.AssembleVolume_Request,    msg.AssembleVolume_Response),
#	"DisassembleVolume":(msg.DisassembleVolume_Request, msg.DisassembleVolume_Response),
#	"RepairVolume":     (msg.RepairVolume_Request,      msg.RepairVolume_Response),
#	"ReadVolume":       (msg.ReadVolume_Request,        msg.ReadVolume_Response),
#	"WriteVolume":      (msg.WriteVolume_Request,       msg.WriteVolume_Response),
#	"MoveVolume":       (msg.MoveVolume_Request,        msg.MoveVolume_Response),
#	"ChMod":            (msg.ChMod_Request,             msg.ChMod_Response),
#	"DeleteVolume":     (msg.DeleteVolume_Request,      msg.DeleteVolume_Response),
#	"CreateLinK":       (msg.CreateLink_Request,        msg.CreateLink_Response),
#}

class ServiceTerminated:
	pass

def BuildMethodInfo(theServer):
	ret={}
	for func in dir(theServer):
		if func.startswith('_') or func.endswith('_'):
			continue
		target=getattr(theServer, func)
		if not inspect.ismethod(target):
			continue
		req=getattr(msg, func+'_Request', None) or getattr(msg, func, type(None))
		res=getattr(msg, func+'_Response', type(None))
		ret[func]=(req, res)
	return ret


def sendRpc(s, guid, token, name, body):
	'''Guid token messageName bodySize\n'''
	line="%s %u %s %u\n" % (Guid.toStr(guid), token, name, len(body))
	s.sendall(line)
	s.sendall(body)
def recvRpc(s):
	fd=s.makefile()
	parts=fd.readline().split()
	if len(parts)==0:
		raise ServiceTerminated()
	guid=Guid.fromStr(parts[0])
	token=int(parts[1])
	name=parts[2]
	size=int(parts[3])
	body=fd.read(size)
	fd.close()
	if len(body)!=size:
		raise IOError('Invalid rpc message')
	return guid,token,name,body

class RpcService:
	def __init__(self, Server, MethodInfo=None):
		self.theServer=Server
		self.peerguid=None
		self.cursocket=None
		Server.service=self
		self.methodInfo=MethodInfo or BuildMethodInfo(Server)
		logging.info(self.methodInfo.keys())
	def peerGuid(self):
		return self.peerguid
	def currentSocket(self):
		return self.cursocket
	
	@staticmethod
	def callMethod(method, argument, rettype):
		ret=None
		logging.debug(type(argument))
		try:
			ret=method(argument)
			assert type(ret)==rettype
		except:
			logging.error("exception ", exc_info=1)
			if rettype!=type(None):
				ret=rettype()
				if hasattr(ret, 'error'):
					ret.error=str(sys.exc_info()[1])
		return ret
	
	def doService(self, socket):
		guid,token,name,body=recvRpc(socket)
		self.peerguid=guid
		self.cursocket=socket
		try:
			MI=self.methodInfo[name]
			argument=MI[0].FromString(body)
			method=getattr(self.theServer, name)
			ret=self.callMethod(method, argument, MI[1])
			if ret==None: return
			body=ret.SerializeToString()
		except:
			name='Exception'
			exctype,value = sys.exc_info()[:2]
			body="{0}\n{1}".format(value.__class__.__name__, value)
			logging.error("exception ", exc_info=1)
		sendRpc(socket, guid, token, name, body)		
	
	def handler(self, socket, address):
		guid=None
		try:
			while True:
				self.doService(socket)
		except ServiceTerminated:
			pass
		finally:
			if guid==None: return
			if hasattr(self.theServer, '_onConnectionClose_'):
				self.theServer._onConnectionClose_()



class RpcStub:
	def __init__(self, guid, socket=None, Interface=None, MethodInfo=None):
		if Interface==None and MethodInfo==None:
			raise ValueError("At least provide an Interface or a MethodInfo")
		self.socket=socket
		self.guid=guid
		self.token=0
		if isinstance(MethodInfo, dict):    #if it's MethodInfo
			self.methodInfo=MethodInfo
		else:
			self.methodInfo=BuildMethodInfo(Interface)
		logging.info(self.methodInfo.keys())
	def callMethod(self, name, argument, socket=None):
		# print name
		# if name == 'DisassembleVolume':
		# 	print 'access_point', argument.access_point
		MI=self.methodInfo[name]
		assert type(argument)==MI[0]
		body=argument.SerializeToString()
		socket = socket or self.socket
		sendRpc(socket, self.guid, self.token, name, body)
		if MI[1]==type(None): return
		guid,token,name_,body_ = recvRpc(socket)
		assert guid==self.guid
		assert token==self.token
		if name_=='Exception':
			raise Exception(body)
		assert name_==name
		self.token=token+1
		ret=MI[1].FromString(body_)

		return ret


###################### Test ####################################
def test_BuildMethodInfo():
	print(MethodInfo)
	class mds:
		def NewChunk():         pass
		def DeleteChunk():      pass
		def NewVolume():        pass
		def AssembleVolume():   pass
		def DisassembleVolume():pass
		def RepairVolume():     pass
		def ReadVolume():       pass
		def WriteVolume():      pass
		def MoveVolume():       pass
		def ChMod():            pass
		def DeleteVolume():     pass
		def CreateLink():       pass
	mi=BuildMethodInfo(mds)
	print(mi)

def test_RpcService():
	pass

if __name__=="__main__":
	logging.basicConfig(level=logging.INFO)
	test_BuildMethodInfo()
	test_RpcService()
