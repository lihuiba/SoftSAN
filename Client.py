import rpc, logging
import messages_pb2 as msg
import client_messages_pb2 as clmsg
import mds, ChunkServer
import gevent.socket
import guid as Guid
import block.dm as dm
from mds import Object
import libiscsi
import scandev

Client_IP = '192.168.0.12'
Mds_IP = '192.168.0.12'
ChunkServer_IP = '192.168.0.12'
Client_Port = 6767
Mds_Port = 2340
ChunkServer_Port = 6780

CHUNKSIZE = 5120
VOLUMEPATH = '/Volume_DB2'

VolumeDictL = {}#Local volume dictionary
VolumeDictM = {}#Mds volume dictionary
volume_list = {}
guid=msg.Guid()
guid.a=12; guid.b=13; guid.c=14; guid.d=15;


def assignVolume(a, b):
	a.size = b.size
	a.assembler = b.assembler
	for param in b.parameters:
		a.parameters.append(param)
	for volume in b.subvolume:
		a.subvolume.append(volume)
	Guid.assign(a,guid, b.guid)

class BuildStub:
	def __init__(self, guid, server, interface):
		self.guid = guid
		self.server = server
		self.interface = interface
	def __enter__(self):
		self.socket = gevent.socket.socket()
		self.socket.connect((self.server.ServiceAddress, self.server.ServicePort))
		return rpc.RpcStub(self.guid, self.socket, self.interface)
	def __exit__(self, a, b, c):
		self.socket.close()
	
class Client:
	
	def GetChunkServers(self, server, count = 5):
		with BuildStub(guid, server, mds.MDS) as stub:
			arg = msg.GetChunkServers_Request()
			arg.randomCount = count
			serverlist = stub.callMethod('GetChunkServers', arg)
		return serverlist

	def NewChunk(self, server, size, count = 1):
		with BuildStub(guid, server, ChunkServer.ChunkServer) as stub:
			arg = msg.NewChunk_Request()
			arg.size = size
			arg.count = count
			chunklist = stub.callMethod('NewChunk', arg)
		return chunklist

    #give a list of chunk sizes, return a list of volumes
    #volume : path node msg.volume
	def NewChunkList(self, chksizes):
		volumelist = []

		serlist = self.GetChunkServers(Mds_IP, Mds_Port)
		print 'serverlist serverlist serverlist serverlist'
		print serlist

		mvolume = msg.Volume()
		lvolume = Object()
		for size in chksizes:
			server = serlist.random[0]
			chunks = self.NewChunk(server, size, 1)
			print 'chunks  chunks chunks  chunks chunks  chunks'
			print chunks

			mvolume.size = size
			lvolume.size = size
			mvolume.assembler = 'chunk'
			Guid.assign(mvolume.guid, chunks.guids[0])
			lvolume.guid = msg.Guid()
			Guid.assign(lvolume.guid, mvolume.guid)

			lvolume.path, lvolume.node = self.MountChunk(server, mvolume)

			key = Guid.toStr(mvolume.guid)
			VolumeDictL[key] = lvolume
			VolumeDictM[key] = mvolume

			print 'volume info volume info volume info volume info '
			print lvolume
			print mvolume

			volumelist.append(lvolume)
		print 'volumelist volumelist volumelist volumelist volumelist'
		print volumelist
		return volumelist

	def MountChunk(self, server, volume):
		with BuildStub(guid, server, ChunkServer.ChunkServer) as stub:
			req = msg.AssembleVolume_Request()
			req.volume.size = volume.size
			Guid.assign(req.volume.guid, volume.guid)
			target = stub.callMethod('AssembleVolume', req)

		nodelist = libiscsi.discover_sendtargets(server.ServiceAddress, 3260)
		if nodelist == None:
			print 'No nodes found!'
		else:
			for node in nodelist:
				if target.access_point == node.name:
					node.login()
					dev = scandev.get_blockdev_by_targetname(node.name)
					return dev, node
		return 'No found!', None

	def unmountChunk(iqn, nodelist):
		for node in nodelist:
			if iqn == node.name:
				node.logout()
				nodelist.remove( node )
				break

	def AssembleVolume(strategy, volumename = ''):
		tablist = []
		start = 0
		for segment in strategy:
			size = segment.size
			dmtype = segment.type
			if dmtype is 'striped':
				params = str(segment.snum) + ' ' + str(segment.stripesize) + ' '
			space = ''
			for chunk in segment.chunklist:
				params += space + chunk.path + ' ' + str(chunk.size)
				space = ' '
			table = dm.table(start, size, dmtype, params)
			tblist.append(table)
			strat += size
		dm.map(volumename, tblist)

	def AssembleLinearVolume(self, volumename, devlist):
		tblist = []
		start = 0
		for dev in devlist:
			size = dev.size
			params = dev.path + ' 0'
			table = dm.table(start, size, 'linear', params)
			tblist.append(table)
			start += size
		# print volumename
		# for table in tblist:
		#  	print table.start; print table.size; print table.params;
		dm.map(volumename, tblist)

	def AssembleStripedVolume(self, volumename, strsize, devlist):
		tblist = []
		start = 0
		size = 0
		params = str( len(devlist) ) + ' ' + str( strsize )
		for dev in devlist:
			size += dev.size
			params += ' ' + dev.path + ' 0'
		print params
		table = dm.table(start, size, 'striped', params)
		tblist.append(table)
		dm.map(volumename, tblist)

	def NewVolume(self, req):
		vollist = []
		mvolume = msg.Volume()

		volname = req.volume_name
		if volname in VolumeDictL:
			print 'volume name has been used! find another one'
		volsize = req.volume_size
		voltype = req.volume_type
		chksizes = req.chunk_size
		volnames = req.subvolume
		params = req.params

		if len(volnames) > 0:
			for name in volnames:
				vollist.append(VolumeDictL[name])
		
		if voltype == '':
			voltype = 'linear'
		if len(chksizes) == 0 and len(subvolume) == 0:
			totsize = volsize
			while totsize > CHUNKSIZE:
				chksizes.append(CHUNKSIZE)
				totsize -= CHUNKSIZE
			chksizes.append(totsize)
		
		if len(chksizes) > 0:
			vollist = self.NewChunkList(chksizes)
		
		if voltype == 'linear':
		 	self.AssembleLinearVolume(volname, vollist)
		else:
		  	if strsize is 0:
		  		strsize = 256
		  	self.AssembleStripedVolume(volname, strsize, vollist)

		mvolume.size = volsize
		mvolume.assembler = voltype
		mvolume.parameters.append(params)
		mvolume.guid = Guid.generate()
		for vol in vollist:
			key = Guid.toStr(vol.guid)
			mvolume.subvolume.append( VolumeDictM[key] )
		key = Guid.toStr(mvolume.guid)
		VolumeDictM[key] = mvolume

		self.WriteVolume(mvolume)

		lvolume = Object()
		lvolume.size = volsize
		lvolume.path = '/dev/mapper'+volname
		lvolume.node = None
		lvolume.guid = msg.Guid()
		Guid.assign(lvolume.guid, mvolume.guid)
		VolumeDictL[volname] = lvolume

		ret = clmsg.NewVolume_Response()
		ret.name = volname
		ret.size = volsize
		return ret

	def DeleteVolumeAll(self, volume):
		pass

	def DeleteVolume(self, req):
		volumename = req.volume_name
		print 'DeleteVolume:volumename is :    ' + volumename
		maplist = dm.maps()
		for mp in maplist:
			if mp.name == volumename:
				mp.remove()
				print 'DeleteleVolume:Disassemble successful'
				break

		print 'DeleteVolume:volume_list is :'
		print volume_list
		nodelist = volume_list[volumename][1]
		for node in nodelist:
			print 'node '+node.name+' is logging out'
			node.logout()
		del volume_list[volumename]
		print 'DeleteleVolume:Dismount nodes successful'

		res = clmsg.DeleteVolume_Response()
		res.result = 'successful'
		return res

	def WriteVolume(self, server, name, newvolume):
		with BuildStub(guid, server, mds.MDS) as stub:
			req = msg.WriteVolume_Request()
			req.volume = newvolume.SerializeToString()
			req.fullpath = '/'+name
			ret = stub.callMethod('WriteVolume', req)

	def DeleteVolume(self, server, path):
		with BuildStub(guid, server, mds.MDS) as stub:
			req = msg.DeleteVolume_Request()
			req.fullpath = path
			ret = stub.callMethod('DeleteVolume', req)

	def ReadVolume(self, name):
		with BuildStub(guid, server, mds.MDS) as stub:
			req = msg.ReadVolume_Request()
			req.fullpath = '/'+name
			ret = stub.callMethod('ReadVolume', req)
		
<<<<<<< HEAD
		volume = msg.Volume.FromString(ret.volume)
		#print 'after parsion:  ', volume
=======
		volume = msg.Volume()
		volume.ParseFromString(ret.volume)

>>>>>>> baf962975a67516f0fe95460f46a540cd3ad004f
		return volume

	def MoveVolume(self, source, dest):
		with BuildStub(guid, server, mds.MDS) as stub:
			req = msg.MoveVolume_Request()
			req.source = source
			req.destination = dest
			ret = stub.callMethod('MoveVolume', req)

def test(server):
	mdsser = Object()
	mdsser.ServiceAddress = Mds_IP
	mdsser.ServicePort = Mds_Port
	serlist = server.GetChunkServers(mdsser)
	print serlist
	# chksize = 10
	# chksizes = [chksize]
	# server.NewChunkList(chksizes)

	# req = clmsg.NewVolume_Request()
	# req.volume_name = 'testlinear'
	# req.volume_size = 20
	# req.chunk_size.append(20)
	# server.NewVolume(req)

if __name__=='__main__':
	logging.basicConfig(level=logging.DEBUG)

	server=Client()
	test(server)
    #################### test read, write volume ###########################
	# server.WriteVolume(5120, 'linear')
	# print 'writevolume done'
	# server.ReadVolume()
     #################### test read, write volume ###########################

	# MI = {}
	# req = getattr(clmsg, 'NewVolume_Request', None)
	# res = getattr(clmsg, 'NewVolume_Response', type(None))
	# MI['NewVolume'] = (req, res)
	# req = getattr(clmsg, 'DeleteVolume_Request', None)
	# res = getattr(clmsg, 'DeleteVolume_Response', type(None))
	# MI['DeleteVolume'] = (req, res)
	# # print MI
	# service=rpc.RpcService(server, MI)
	# framework=gevent.server.StreamServer(('0.0.0.0', Client_Port), service.handler)
	# framework.serve_forever()