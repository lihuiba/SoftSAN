import redis, inspect, logging
import messages_pb2 as msg
from greenlet import greenlet
import time, collections

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
CHANNEL_HEARTBEAT=CHANNEL+'.heartbeat'

def CHANNEL_fromGuid(guid):
	global CHANNEL
	if guid==None or (guid.a==0 and guid.b==0 and guid.c==0 and guid.d==0):
		return CHANNEL_MDS
	client="{0}.{1}.{2}.{3}.{4}".format(CHANNEL, \
		guid.a, guid.b, guid.c, guid.d)
	return client

def BuildMethodInfo(theServer):
	ret={}
	for func in dir(theServer):
		if func.startswith('__'):
			continue
		req=getattr(msg, func+'_Request', type(None))
		res=getattr(msg, func+'_Response', type(None))
		ret[func]=(req, res)
	return ret

class ServiceTerminated:
	pass
class ServiceTimeout:
	pass

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

	#request must be an instance of msg.Header in the request form
	def callMethod(self, request):
		methodname=request.method
		argument=self.methodInfo[methodname][0]()
		argument.ParseFromString(request.argument)
		method=getattr(self.theServer, methodname)
		ret=method(argument)
		rettype=self.methodInfo[methodname][1]
		if not isinstance(ret, rettype):
			raise TypeError('return value of {0} is supposed to be {1}, but in fact {2}'. \
				format(methodname, rettype, type(ret)))
		return ret

	def doListen(self):
		return self.rclient.listen()

	def listen(self, channelGuids=None):
		channels=None
		if channelGuids==None:
			self.rclient.subscribe(CHANNEL_MDS)
			self.rclient.subscribe(CHANNEL_HEARTBEAT)
			channels=(CHANNEL_MDS, CHANNEL_HEARTBEAT)
			logging.info("subscribed to channel %s.", channels)
		else:
			channels=[]
			if not isinstance(channelGuids, collections.Iterable):
				channelGuids=(channelGuids,)
			for guid in channelGuids:
				channel=CHANNEL_fromGuid(guid)
				self.rclient.subscribe(channel)	
				channels.append(channel)
				logging.info("subscribed to channel '%s'", channel)

		for message in self.doListen():	
			logging.debug(message)
			mtype=message['type']
			mdata=message['data']
			if mtype=='subscribe': 
				continue
			elif mtype=='unsubscribe':
				raise ServiceTerminated()
			elif mtype!='message' or not (message['channel'] in channels):
				assert False, "should receive messages from channels '{0}'".format(channels)
				continue
			elif not isinstance(mdata, str):
				assert False, "type of message data should be str!"
				continue
			header=msg.Header.FromString(mdata)
			if header.isRequest:
				self.processRequest(header)
			else:
				self.processResponse(header)

	def processRequest(self, request):
		logging.debug("RpcServiceCo: processRequest")
		ret=self.callMethod(request)
		if ret!=None:
			response=msg.Header()
			response.token=request.token
			response.caller.a=request.caller.a
			response.caller.b=request.caller.b
			response.caller.c=request.caller.c
			response.caller.d=request.caller.d
			response.errorno=0
			response.isRequest=False
			response.ret=ret.SerializeToString()
			caller=CHANNEL_fromGuid(request.caller)
			logging.debug("replying to %s", caller)
			self.rclient_pub.publish(caller, response.SerializeToString())

	def processResponse(self, response):
		raise NotImplementedError("RpcService doesn't process responses!")


class RpcServiceCo(RpcService):
	sched=None
	def __init__(self, scheduler, MDSServer, MethodInfo=None):
		self.sched=scheduler
		RpcService.__init__(self, MDSServer, MethodInfo)
	def processRequest(self, request):
		def starter():
			try:
				RpcService.processRequest(self, request)
			except:
				pass
			self.sched.prepareDie()
		self.sched.createThread(starter)
		logging.debug("RpcServiceCo: service thread created")
	def doListen(self):
		#return self.sched.doListen(self.rclient)
		for msg in self.rclient.listen():
			yield msg
			logging.debug("RpcServiceCo.doListen, thread count: %d", len(self.sched.activeq))
			while len(self.sched.activeq)>1:
				self.sched.switch()
	responseRegistry={}
	@staticmethod
	def makeKey(h):
		return (h.token, h.caller.a, h.caller.b, h.caller.c, h.caller.d)		
	def processResponse(self, response):
		key=self.makeKey(response)
		logging.debug("processResponse: %s", key)
		if not key in self.responseRegistry:
			debug.info("session not found in the registry! (%s)", key)
			return
		record=self.responseRegistry[key]
		del self.responseRegistry[key]
		record[4]=response
		th=record[3]
		self.sched.active(th)
	def registerResponse(self, record):
		key=self.makeKey(record[0])
		record[3]=Scheduler.getCurrent()
		self.responseRegistry[key]=record
		logging.debug("registered session: %s", key)


class RpcStub:
	token=0;
	methodInfo=None
	rclient=None
	rclient_pub=None
	guid=None
	callWindow={}
	callee=None
	def __init__(self, guid, Interface=None, MethodInfo=None):
		if Interface==None and MethodInfo==None:
			raise ValueError("At least provide a Interface or a MethodInfo")
		self.guid=guid
		self.rclient_pub=redis.client.Redis()
		self.rclient=redis.client.Redis()
		self.myChannel=CHANNEL_fromGuid(guid)
		self.rclient.subscribe(self.myChannel)
		self.callee=CHANNEL_MDS				#default callee
		if isinstance(MethodInfo, dict):    #if it's MethodInfo
			self.methodInfo=MethodInfo
		else:
			self.methodInfo=BuildMethodInfo(Interface)
		logging.info(self.methodInfo.keys())
	def setCallee(self, guid):
		self.callee=CHANNEL_fromGuid(guid)

	def callMethod_async(self, method, argument, done=None):
		req=msg.Header()
		req.token=self.token
		self.token=self.token+1
		req.isRequest=True;
		req.method=method
		req.caller.a=self.guid.a
		req.caller.b=self.guid.b
		req.caller.c=self.guid.c
		req.caller.d=self.guid.d
		req.argument=argument.SerializeToString()
		data=req.SerializeToString()
		logging.debug("calling %s", self.callee)
		self.rclient_pub.publish(self.callee, data)
		record=[req, argument, done, None, None]		# the two Nones will be current thread and response
		self.callWindow[req.token]=record
		return record
	def bottom_half(self, response):
		record=self.callWindow[response.token]
		methodName=record[0].method
		responseType=self.methodInfo[methodName][1]
		ret=responseType.FromString(response.ret)
		del self.callWindow[response.token]
		done=record[2]
		if done: done(ret)
		return record, ret
	def doMessage(self):
		for message in self.rclient.listen():
			logging.debug(message)
			mtype=message['type']
			mdata=message['data']
			if mtype=='subscribe': 
				continue
			elif mtype=='unsubscribe':
				return
			elif mtype!='message' or message['channel']!=self.myChannel:
				assert False, "should receive messages from channel '{0}'".format(self.myChannel)
				continue
			elif not isinstance(mdata, str):
				assert False, "type of message data should be str!"
				continue
			response=msg.Header.FromString(mdata)
			return self.bottom_half(response)
	def callMethod(self, method, argument):
		record0=self.callMethod_async(method, argument)
		while True:
			record1,ret = self.doMessage()
			if record0==record1:
				return ret
	def callMethod_callee(self, method, argument, callee):
		self.setCallee(callee)
		self.callMethod(method, argument)

class RpcStubCo(RpcStub):
	sched=None
	serviceCo=None
	def __init__(self, guid, scheduler, Interface=None, MethodInfo=None):
		RpcStub.__init__(self, guid, Interface, MethodInfo)
		self.sched=scheduler
	def resume(self, thread, ret, retv):
		logging.debug("return value: %s", retv)
		ret[0]=retv
		self.sched.active(thread)
	def callMethod(self, method, argument):
		ret=[0,]
		current=greenlet.getcurrent()
		done = lambda retv : self.resume(current, ret, retv)
		record=self.callMethod_async(method, argument, done)
		if self.serviceCo:
			self.serviceCo.registerResponse(record)		
		self.sched.suspend(current)

		# here, when get resumed, record[4] will be response
		response=record[4]
		self.bottom_half(response)
		return ret[0]
	def setServiceCo(self, service):
		self.serviceCo=service


class Scheduler:
	activeq=[]		# activeq[0] will always be the current thread
	suspendq=[]
	timing={}
	def __init__(self):
		current=greenlet.getcurrent()
		self.activeq.append(current)
	@staticmethod
	def getCurrent():
		th=greenlet.getcurrent()
		return th
	def createThread(self, func):
		th=greenlet(func)
		self.activeq.append(th)
		self.timing[th]=time.time()
		return th
	def clearTimeoutThreads(self, maxLife=None):
		if maxLife==None:
			maxLife=30 #seconds
		now=time.time()
		for th in self.timing:
			age=now-self.timing[th]
			if age>maxLife:
				th.throw()
	def active(self, thread):
		if thread in self.activeq:
			return
		logging.debug("activing %d", hash(thread))
		self.activeq.append(thread)
		self.suspendq.remove(thread)
	def suspend(self, thread=None):
		if thread in self.suspendq:
			return
		current=self.activeq[0]
		if thread==None:
			thread=current
		logging.debug("suspending %d", hash(thread))
		self.activeq.remove(thread)
		self.suspendq.append(thread)
		if thread==current:
			self.activeq[0].switch()
	def prepareDie(self):
		th=self.activeq.pop(0)
		th.parent=self.activeq[0]
		del self.timing[th]
		logging.debug("preparing thread %d to die.", hash(th))
	def switch(self):
		c=self.activeq.pop(0)
		self.activeq.append(c)
		self.activeq[0].switch()
	def doListen(self, r):
		for msg in r.listen():
			logging.debug("Scheduler.doListen, thread count: %d", len(self.activeq))
			while len(self.activeq)>1:
				self.switch()
			yield msg
			


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
