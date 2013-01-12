import logging, random
import messages_pb2 as msg
import guid as Guid
import transientdb
import persistentdb
import gevent.socket
import ChunkServer
from util import *
import rpc, config

def splitbyattr(objs, key):
	objs.sort(key = lambda x : getattr(x, key))
	group=None
	attr=None
	for p in objs:
		value=getattr(p, key)
		if attr==None:
			group=[p, ]
			attr=value
			continue
		elif attr==value:
			group.append(p)
		elif attr!=value:
			yield group
			group=[p, ]
			attr=value
	if group!=None:
		yield group

class MDS:
	def __init__(self, transientdb, persistentdb):
		self.service=None
		self.tdb=transientdb
		self.pdb=persistentdb
		socket=gevent.socket.socket()
		guid=Guid.generate()
		self.stub=rpc.RpcStub(guid, socket, ChunkServer.ChunkServer)

	def ChunkServerInfo(self, arg):
		# print arg
		cksinfo=message2object(arg)
		cksinfo.guid=Guid.toStr(self.service.peerGuid())
		self.tdb.putChunkServer(cksinfo)
		# print cksinfo.__dict__

	def GetChunkServers(self, arg):
		ret=msg.GetChunkServers_Response()
		servers=self.tdb.getChunkServerList()
		servers=self.tdb.getChunkServers(servers)
		for s in servers:
			t=ret.random.add()
			t.ServiceAddress=s.ServiceAddress
			t.ServicePort=int(s.ServicePort)
		return ret

	def __ForwardNewChunk__(self, arg, target, aresponse):
		self.stub.socket.connect((target.ServiceAddress, target.ServicePort))
		ret0=self.stub.callMethod('NewChunk', arg)
		self.stub.socket.close()
		if aresponse:
			for guid in ret0.guids:
				t=aresponse.guids.add()
				Guid.assign(t, guid)
			if ret0.error:
				t=[group[0].guid, group[0].ServiceAddress, group[0].ServicePort, ret0.error]
				aresponse.error="error from chunk server {0} @ ({1}:{2}):\n{3}".format(*t)
		return ret0

	def __NewChunkRandom__(self, arg):
		servers=self.tdb.getChunkServerList()
		servers=self.tdb.getChunkServers(servers)
		random.shuffle(servers)
		count=arg.count
		per=count/len(servers)
		p=float(count-per*len(servers))/len(servers)
		aresponse=msg.NewChunk_Response()
		Guid.setZero(arg.guid)
		for server in servers:
			if server!=servers[-1]:
				arg.count=per
				if random.random()<p:
					arg.count+=1
			else:
				arg.count=count-len(aresponse.guids)
			if arg.count+len(aresponse.guids)>count:
				arg.count=count-len(aresponse.guids)
			ret0=self.__ForwardNewChunk__(arg, server, aresponse)
			if ret0.error or len(aresponse.guids)==count: 
				break
		return aresponse

	def NewChunk(self, arg):
		if Guid.isZero(arg.guid):
			return self.__NewChunkRandom__(arg)
		else:
			server==self.tdb.getChunkServers(arg.guid)
			if len(server)==0:		#not found in the server list
				return self.__NewChunkRandom__(arg)
			return self.__ForwardNewChunk__(arg, server, None)

	# def DeleteChunk(self, arg):
	#	 logging.debug(type(arg))
	#	 chunks=self.tdb.getChunks(arg.guids)
	#	 done=[]
	#	 for cgroup in splitbyattr(chunks, 'serverid'):
	#		 arg.guids.clear()
	#		 for c in cgroup:
	#			 guid=guidFromStr(c.guid)
	#			 arg.guids.add()
	#			 guidAssign(arg.guids[-1], guid)
	#		 ret=self.stub.callMethod_on("DeleteChunk", arg, cgroup[0].serverid)
	#		 done=done+ret.guids
	#		 if ret.error:
	#			 break;
	#	 ret.guids=done
	#	 return ret
	# def AssembleVolume(self, arg):
	#	 logging.debug(type(arg))
	#	 ret=msg.AssembleVolume_Response()
	#	 ret.access_point='no access_point'
	#	 return ret
	# def DisassembleVolume(self, arg):
	#	 logging.debug(type(arg))
	#	 ret=msg.DisassembleVolume_Response()
	#	 ret.access_point=arg.access_point
	#	 return ret
	# def RepairVolume(self, arg):
	#	 logging.debug(type(arg))
	#	 ret=msg.RepairVolume_Response()
	#	 return ret
	def ReadVolume(self, arg):
		ret=msg.ReadVolume_Response()
		ret.volume=self.pdb.read(arg.fullpath)
		return ret
	def WriteVolume(self, arg):
		ret=msg.WriteVolume_Response()
		self.pdb.write(arg.fullpath, arg.volume)
		return ret
	def MoveVolume(self, arg):
		ret=msg.MoveVolume_Response()
		self.pdb.move(arg.source, arg.destination)
		return ret
	# def ChMod(self, arg):
	#	 ret=msg.ChMod_Response()
	#	 return ret
	def DeleteVolume(self, arg):
		ret=msg.DeleteVolume_Response()
		self.pdb.delete(arg.fullpath)
		return ret
	def CreateDirectory(self, arg):
		ret=msg.CreateDirectory_Response()
		self.pdb.createDirectory(arg.fullpath, arg.parents)
		return ret
	def DeleteDirectory(self, arg):
		ret=msg.DeleteDirectory_Response()
		self.pdb.deleteDirectory(arg.fullpath, arg.recursively_forced)
		return ret
	def ListDirectory(self, arg):
		ret=msg.ListDirectory_Response()
		listdir=self.pdb.listDirectory(arg.fullpath)
		for item in listdir:
			it=ret.items.add()
			object2message(item, it)
		return ret


#############################################################################

def buildMDS():
	import filesystem
	testdir=filesystem.test_prepare_dir('/mds')
	pdb=persistentdb.PersistentDB(testdir)
	tdb=transientdb.TransientDB()
	server=MDS(tdb, pdb)
	return server	

def test_main():
	import gevent.server, rpc
	server=buildMDS()
	service=rpc.RpcService(server)
	framework=gevent.server.StreamServer(('0.0.0.0', 0x6789), service.handler)
	framework.serve_forever()


def test_split():
	o1={ 'asdf':123, 'des':823}
	o2={ 'asdf':231, 'des':238}
	o3={ 'asdf':312, 'des':382}
	lst=[Object(o1), Object(o2), Object(o3)]	
	for group in splitbyattr(lst, 'des'):
		for c in group:
			print c.__dict__

def test():
	server=buildMDS()
	arg=Object()
	arg.fullpath='/asdf'
	arg.volume='hahahahha'
	ret=server.WriteVolume(arg)
	print ret
	ret=server.ReadVolume(arg)
	print ret
	arg=Object()
	arg.fullpath='/nnd/kkk/mdk'
	arg.parents=True
	ret=server.CreateDirectory(arg)
	print ret
	arg.fullpath='/mds'
	ret=server.CreateDirectory(arg)
	print ret
	arg=Object()
	arg.fullpath='/'
	ret=server.ListDirectory(arg)
	print ret
	arg.fullpath='/nnd'
	arg.recursively_forced=True
	ret=server.DeleteDirectory(arg)
	print ret



if __name__=="__main__":
	logging.basicConfig(level=logging.DEBUG)
	test_main()
	#test_split()
	#test()
