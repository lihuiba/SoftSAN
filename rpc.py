import redis, inspect, logging
import messages_pb2 as msg

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


CHANNEL='SoftSAN.RPC.0'
CHANNEL_MDS=CHANNEL+'.MDS'

def CHANNEL_Client(guid):
	global CHANNEL
	client="{0}.{1}.{2}.{3}.{4}".format(CHANNEL, \
		guid.a, guid.b, guid.c, guid.d)
	return client

def BuildMethodInfo(theServer):
	ret={}
	for func in dir(theServer):
		if func.startswith('__'):
			continue
		req=getattr(msg, func+'_Request', None)
		res=getattr(msg, func+'_Response', None)
		if inspect.isclass(req) and inspect.isclass(res):
			ret[func]=(req, res)
		# else:
		# 	print func, req, res
	return ret

class RpcService:
	theServer=None
	methodInfo=None
	rclient=None
	rclient_pub=None
	def __init__(self, MDSServer, MethodInfo=None):
		self.theServer=MDSServer
		self.rclient=redis.client.Redis()
		self.rclient_pub=redis.client.Redis()
		self.methodInfo=MethodInfo or BuildMethodInfo(MDSServer)
		logging.info(self.methodInfo)

	#request must be an instance of msg.Request
	def callMethod(self, request):
		methodname=request.method
		argument=self.methodInfo[methodname][0]()
		argument.ParseFromString(request.argument)
		method=getattr(self.theServer, methodname)
		ret=method(argument)
		rettype=self.methodInfo[methodname][1]
		if not isinstance(ret, rettype):
			raise TypeError('return value of {0} is supposed to be {1}, but in facet {2}'. \
				format(methodname, rettype, type(ret)))
		return ret

	def listen(self):
		global CHANNEL_MDS
		self.rclient.subscribe(CHANNEL_MDS)
		for message in self.rclient.listen():
			print message
			mtype=message['type']
			mdata=message['data']
			if mtype=='subscribe': 
				continue
			elif mtype=='unsubscribe':
				break
			elif mtype!='message' or message['channel']!=CHANNEL_MDS:
				assert False, "should receive messages from channel '{0}'".format(CHANNEL_MDS)
				continue
			elif not isinstance(mdata, str):
				assert False, "type of message data should be str!"
				continue
			request=msg.Request.FromString(mdata)
			
			ret=self.callMethod(request)
			
			response=msg.Response()
			response.token=request.token
			response.errorno=0
			response.ret=ret.SerializeToString()
			caller=CHANNEL_Client(request.caller)
			self.rclient_pub.publish(caller, response.SerializeToString())

class RpcStub:
	token=0;
	theServer=None
	methodInfo=None
	rclient=None
	rclient_pub=None
	guid=None
	callWindow={}
	def __init__(self, MDSServer, guid, MethodInfo=None):
		self.guid=guid
		self.rclient_pub=redis.client.Redis()
		self.rclient=redis.client.Redis()
		self.myChannel=CHANNEL_Client(guid)
		self.rclient.subscribe(self.myChannel)
		self.theServer=MDSServer
		self.methodInfo=MethodInfo or BuildMethodInfo(MDSServer)
		logging.info(self.methodInfo)

	#request must be an instance of msg.Request
	def callMethod_async(self, method, argument, done=None):
		global CHANNEL_MDS
		req=msg.Request()
		req.token=self.token
		self.token=self.token+1
		req.method=method
		req.caller.a=self.guid.a
		req.caller.b=self.guid.b
		req.caller.c=self.guid.c
		req.caller.d=self.guid.d
		req.argument=argument.SerializeToString()
		data=req.SerializeToString()
		self.rclient_pub.publish(CHANNEL_MDS, data)
		record=(req, argument, done)
		self.callWindow[req.token]=record
		return record

	def wait(self):
		for message in self.rclient.listen():
			print message
			mtype=message['type']
			mdata=message['data']
			if mtype=='subscribe': 
				continue
			elif mtype=='unsubscribe':
				break
			elif mtype!='message' or message['channel']!=self.myChannel:
				assert False, "should receive messages from channel '{0}'".format(self.myChannel)
				continue
			elif not isinstance(mdata, str):
				assert False, "type of message data should be str!"
				continue
			res=msg.Response.FromString(mdata)
			record=self.callWindow[res.token]
			responseType=self.methodInfo[record[0].method][1]
			ret=responseType.FromString(res.ret)
			del self.callWindow[res.token]
			done=record[2]
			if done: done(ret)
			return record, ret

	def callMethod(self, method, argument):
		record0=self.callMethod_async(method, argument)
		while True:
			record1,ret = self.wait()
			if record0==record1:
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
