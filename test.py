# # import Client

# def test_chkserv():
# 	csc = Client.ChunkServerClient()
import block.dm

g = 1

def test_dic():
	# global g
	t = (1,2,3,4)
	l = list(t)
	l1 = (1,[1,1])
	l2 = (2,[2,2])
	l3 = (3,[3,3])
	ll = (l1,l2,l3)
	d = dict(l for l in ll)
	print d
	g = 2

def print_g():
	# global g
	# g = 3
	print g

class myclass():
	def __init__(self, m):
		self.__m__ = m
	def __set(self, m):
		self.__m__ = m
	def _get(self, m):
		return self.__m__
	def incre_m(self):
		self.__m__ = self.__m__ + 1

def test_dir():
	o = myclass(1)
	print dir()
	print dir(o)
	dic = {}
	dic['name'] = 'mingtai'
	dic['date'] = '2013-01-09'
	dic['weather'] = 'snow'
	print dic
	if dic.has_key('date'):
		print 'has key %s' %('date')
	

def test(*args, **kwargs):
	print args
	print kwargs
	# print kwargs['a']


if __name__ == '__main__':
	test_dir()
	
	

# class ChunkServerClient:
# 	# class-level members
# 	pool = Pool(createSocket, closeSocket)
# 	MI=rpc.BuildMethodInfo(ChunkServer.ChunkServer)
	
# 	def __init__(self, guid, csip, csport):
# 		#assert hasattr(ChunkServerClient, 'guid')
# 		self.guid = guid
# 		self.endpoint=(csip, csport)
	
# 	def getStub(self):
# 		if hasattr(self, 'stub'):
# 			return self.stub
# 		socket=self.pool.get(self.endpoint)
# 		stub=rpc.RpcStub(self.guid, socket, ChunkServer.ChunkServer)
# 		self.stub=stub
# 		return stub

# 	def NewChunk(self, size, count = 1):
# 		arg = msg.NewChunk_Request()
# 		arg.size = size
# 		arg.count = count
# 		stub = self.getStub()
# 		chunklist = stub.callMethod('NewChunk', arg)
# 		return chunklist

# 	def DeleteChunk(self, volumes):
# 		stub = self.getStub()
# 		arg = msg.DeleteChunk_Request()
# 		if not isinstance(volumes, list):
# 			volumes = [volumes]
# 		for volume in volumes:
# 			t = arg.guids.add()
# 			volguid = Guid.fromStr(volume.guid)
# 			Guid.assign(t, volguid)
# 		ret = stub.callMethod('DeleteChunk', arg)

# 	def AssembleChunk(self, volume):
# 		stub = self.getStub()
# 		req = msg.AssembleVolume_Request()
# 		obj2msg(volume, req.volume)
# 		target = stub.callMethod('AssembleVolume', req)
# 		return target

# 	def DisassembleChunk(self, nodename):
# 		stub = self.getStub()
# 		req = msg.DisassembleVolume_Request()
# 		req.access_point = nodename
# 		# retrun value include access point of volume
# 		ret = stub.callMethod('DisassembleVolume', req)

# 	def Clear():
# 		self.pool.dispose()
