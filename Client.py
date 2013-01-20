import rpc, logging, sys
import messages_pb2 as msg
import mds, ChunkServer
import gevent.socket
import guid as Guid
import block.dm as dm
from mds import Object
import libiscsi, os
import scandev, config
import ClientDeamon
from util import message2object as msg2obj
from util import object2message as obj2msg
from util import Pool
from ClientDeamon import DMClient
from collections import Iterable
import util, config

#branches of volume tree
#vertical, up and right, vertical and right, horizontal, horizontal and down
branch = [
		'\342\224\202',\
		'\342\224\224',\
		'\342\224\234',\
		'\342\224\200',\
		'\342\224\254' \
]

#default chunk size
CHUNKSIZE = 64

class MDSClient:
	def __init__(self, guid, mdsip, mdsport):
		self.socket = gevent.socket.socket()
		self.socket.connect((mdsip, mdsport))
		self.stub = rpc.RpcStub(guid, self.socket, mds.MDS)
	
	def GetChunkServers(self, count=None):
		arg = msg.GetChunkServers_Request()
		if count:
			arg.randomCount = count
		ret = self.stub.callMethod('GetChunkServers', arg)
		serverlist=[msg2obj(x) for x in ret.random]
		return serverlist

	def WriteVolumeInfo(self, volume):
		path = volume.parameters[0]
		req = msg.WriteVolume_Request()
		msgvolume = msg.Volume()
		obj2msg(volume, msgvolume)
		req.volume = msgvolume.SerializeToString()
		req.fullpath = '/'+path
		ret = self.stub.callMethod('WriteVolume', req)

	def DeleteVolumeInfo(self, volume):
		if isinstance(volume, str):
			path = volume
		else:
			path = volume.parameters[0]
		req = msg.DeleteVolume_Request()
		req.fullpath = '/'+path
		ret = self.stub.callMethod('DeleteVolume', req)

	def ReadVolumeInfo(self, name):
		if isinstance(name, str):
			path = name
		else:
			path = Guid.toStr(name)
		req = msg.ReadVolume_Request()
		req.fullpath = '/'+path
		try:
			ret = self.stub.callMethod('ReadVolume', req)
		except Exception as ex:
			index = str(ex).find(path)
			if index > -1:
				return None
			logging.error(str(ex))
		volume = msg.Volume()
		volume.ParseFromString(ret.volume)
		return msg2obj(volume)

	def MoveVolumeInfo(self, source, dest):
		req = msg.MoveVolume_Request()
		req.source = source
		req.destination = dest
		ret = self.stub.callMethod('MoveVolume', req)


class ChunkServerClient:
	
	def __init__(self, guid, csip, csport):
		self.__class__.guid=guid
		self.endpoint=(csip, csport)
	
	def getStub(self):
		self.socket = gevent.socket.socket()
		self.socket.connect(self.endpoint)
		stub=rpc.RpcStub(self.guid, self.socket, ChunkServer.ChunkServer)
		self.stub=stub
		def getStub_New(self):
			return self.stub
		self.getStub=getStub_New
		return stub

	def NewChunk(self, size, count = 1):
		arg = msg.NewChunk_Request()
		arg.size = size
		arg.count = count
		stub = self.getStub()
		chunklist = stub.callMethod('NewChunk', arg)
		return chunklist

	def DeleteChunk(self, volumes):
		stub = self.getStub()
		arg = msg.DeleteChunk_Request()
		if not isinstance(volumes, list):
			volumes = [volumes]
		for volume in volumes:
			t = arg.guids.add()
			volguid = Guid.fromStr(volume.guid)
			Guid.assign(t, volguid)
		ret = stub.callMethod('DeleteChunk', arg)

	def AssembleChunk(self, volume):
		# if isinstance(volume, msg.Volume):
		# 	volume = msg2obj(volume)
		stub = self.getStub()
		req = msg.AssembleVolume_Request()
		obj2msg(volume, req.volume)
		target = stub.callMethod('AssembleVolume', req)
		return target

	def DisassembleChunk(self, nodename):
		stub = self.getStub()
		req = msg.DisassembleVolume_Request()
		req.access_point = nodename
		# retrun value include access_point of volume
		ret = stub.callMethod('DisassembleVolume', req)
		return True

	def Clear(self):
		self.socket.close()
		del self.getStub
		del self.socket
		del self.stub


class Client:
	def __init__(self, mdsip, mdsport):
		self.guid = Guid.generate()
		self.chkpool = Pool(ChunkServerClient, ChunkServerClient.Clear)
		self.mds = MDSClient(self.guid, mdsip, mdsport)
		#self.dmclient = DMClient()
		#self.socket = gevent.socket.socket()
		#self.socket.connect(('192.168.0.12', 6767))
		#self.stub = rpc.RpcStub(self.guid, self.socket, ClientDeamon.ClientDeamon)

		#self.RestoreVolumeInfo()
		
	# give a list of chunk sizes, return a list of volumes
	# volume : path node msg.volume
	def NewChunkList(self, chksizes):
		volumelist = []
		
		serlist = self.mds.GetChunkServers()

		for size in chksizes:
			server = serlist[0]
			addr = server.ServiceAddress
			port = server.ServicePort
			chkclient = self.chkpool.get(self.guid, addr, port)
			chunks = chkclient.NewChunk(size, 1)
			if chunks == []:
				pass
			
			volume = Object()
			volume.size = size
			volume.assembler = 'chunk'
			volume.guid = Guid.toStr(chunks.guids[0])
			volume.parameters = [volume.guid, '', '', addr, str(port)]
			path, nodename = self.MountChunk(volume)
			volume.parameters[1] = path
			volume.parameters[2] = nodename
			
			if path == None:
				for volume in volumelist:
					self.UnmountChunk(volume)
				logging.error('Mount chunk failed')
				return None

			volumelist.append(volume)
		return volumelist

	def GetChunkNode(self, name, addr, port=3260):
		nodelist = libiscsi.discover_sendtargets(addr, port)
		for node in nodelist:
			if name == node.name:
				return node
		return None

	def MountChunk(self, volume):
		addr = volume.parameters[3]
		port = int(volume.parameters[4])
		chkclient = self.chkpool.get(self.guid, addr, port)
		target = chkclient.AssembleChunk(volume)
		node = self.GetChunkNode(target.access_point, addr)
		if not node == None:
			try:
				node.login()
			except Exception as ex:
				ex_str = str(ex)
				index = ex_str.find('session exists')
				if index == -1:
					logging.error(ex_str)
					return None, None
				else:
					logging.warn('chunk '+node.name+' is already mounted')
					print 'chunk '+node.name+' is already mounted'
			dev = scandev.get_blockdev_by_targetname(node.name)
			if not dev == None:
				return dev, node.name
		return None, None

	def UnmountChunk(self, volume):
		addr = volume.parameters[3]
		port = int(volume.parameters[4])
		nodename = volume.parameters[2]
		node = self.GetChunkNode(nodename, addr)
		if node == None:
			return False
		node.logout()
		chkclient = self.chkpool.get(self.guid, addr, port)
		if chkclient.DisassembleChunk(nodename) == False:
			return False
		return True
		
	def MountVolume(self, volume_name):
		volume = self.mds.ReadVolumeInfo(volume_name)
		if isinstance(volume, type(None)):
			logging.error('Volume does not exist: '+volume_name)
			return False
		if self.MountVolumeTree(volume) == False:
			return False
		if self.dmclient.MountVolume(volume) == False
			return False
		return True

	def MountVolumeTree(self, volume):
		if volume.assembler == 'chunk':
			ret = self.MountChunk(volume)
			if ret[0] == None:
				return False
			return True
		for subvol in volume.subvolumes:
			if self.MountVolumeTree(subvol) == False:
				return False
		return True

	def UnmountVolume(self, volume_name):
		volume = self.mds.ReadVolumeInfo(volume_name)
		if isinstance(volume, type(None)):
			logging.error('Volume does not exist: '+volume_name)
			return False
		if self.dmclient.DeleteVolume(volume) == False:
			return False
		if self.UnmountVolumeTree(volume) == False:
			return False
		return True

	def UnmountVolumeTree(self, volume):
		if volume.assembler == 'chunk':
			return self.UnmountChunk(volume)
		for subvol in volume.subvolumes:
			if self.UnmountVolumeTree(subvol) == False:
				return False
		return True

	def SplitVolume(self, volumename):
		volume = self.mds.ReadVolumeInfo(volumename)
		if isinstance(volume, type(None)):
			logging.error('Volume does not exist: '+volumename)
			return False
		if volume.subvolumes[0].assembler == 'chunk':
			logging.error('This volume is not divisible!')
			return False

		if self.dmclient.SplitVolume(volume) == False:
			return False
		self.mds.DeleteVolumeInfo(volume)
		for subvol in volume.subvolumes:
			subvol.parameters[2] = 'free'
			self.mds.WriteVolumeInfo(subvol)
		return True

	def CreateVolume(self, arg):
		vollist = []
		volume = Object()

		volname = arg.name
		volsize = arg.size
		voltype = arg.type
		chksizes = arg.chunksizes
		volnames = arg.subvolumes
		params = arg.parameters

		if len(volnames) > 0:
			for name in volnames:
				vol = self.mds.ReadVolumeInfo(name)
				if isinstance(vol, type(None)):
					logging.error('Volume does not exist: '+name)
					return False
				if vol.parameters[2] == 'used':
					print 'volume '+name+' has been used'
					return False
				if vol == None:
					return False
				vollist.append(vol)
		
		if len(chksizes) == 0 and len(volnames) == 0:
			totsize = volsize
			while totsize > CHUNKSIZE:
				chksizes.append(CHUNKSIZE)
				totsize -= CHUNKSIZE
			chksizes.append(totsize)

		if voltype == '':
			voltype = 'linear'

		if voltype == 'gfs':
			temlist = []
			for chksize in chksizes:
				temlist.append(chksize)
				temlist.append(chksize)
				temlist.append(chksize)
			chksizes = temlist

		if len(chksizes) > 0:
			vollist = self.NewChunkList(chksizes)
			if vollist == None:
				logging.error('New Chunk failed')
				return False
			for vol in vollist:
				print 'vol.size: ', vol.size

		volume.size = volsize
		volume.assembler = voltype
		volume.subvolumes = vollist
		volume.guid = Guid.toStr(Guid.generate())
		volume.parameters = [volname, '/dev/mapper/'+volname, 'free']
		volume.parameters.extend(params)

		ret = self.dmclient.MapVolume(volume)
		if ret == False:
			logging.error('Map volume failed')
			if volume.subvolumes[0].assembler == 'chunk':
				for subvol in volume.subvolumes:
					self.DeleteVolumeTree(subvol)
			return False

		self.mds.WriteVolumeInfo(volume)
		if len(volnames) > 0:
			for vol in vollist:
				vol.parameters[2] = 'used'
		for vol in vollist:
			self.mds.WriteVolumeInfo(vol)
		return True
 
	def DeleteVolume(self, name):
		volume = self.mds.ReadVolumeInfo(volume)
		if isinstance(volume, type(None)):
			logging.error('Volume does not exist'+volume)
			return False
		if isinstance(name, Object):
			name = name.parameters[0]
		self.DeleteVolumeTree(name)
		self.dmclient.DeleteVolume(volume)

	def DeleteVolumeTree(self, volume):
		if volume.assembler == 'chunk':
			addr = volume.parameters[3]
			port = int(volume.parameters[4])
			chkclient = self.chkpool.get(self.guid, addr, port)
			self.UnmountChunk(volume)
			chkclient.DeleteChunk(volume)
			return True
		for subvolume in volume.subvolumes:
			if self.DeleteVolumeTree(subvolume) == False:
				return False
		self.mds.DeleteVolumeInfo(volume)
		return True

	def ListVolume(self, data):
		volume = self.mds.ReadVolumeInfo(data.name)
		if isinstance(volume, type(None)):
			logging.error('Volume does not exist'+data.name)
			return False
		space = '      '
		print 'name:', space, volume.parameters[0]
		print 'path:', space, volume.parameters[1]
		print 'size:', space, volume.size, 'MB'
		if data.tree == '1':
			self.print_tree(volume, '')

	def print_tree(self, volume, prefix):
		print os.path.basename(volume.parameters[1])
		if hasattr(volume, 'subvolumes'):
			num = len(volume.subvolumes)
		else:
			num = 0
		i = 0
		for i in range(0, num):
			if i == num-1:
				print prefix+' '+branch[1],
				add = '  '
			else:
				print prefix+' '+branch[2],
				add = ' '+branch[0]
			self.print_tree(volume.subvolumes[i], prefix+add)

	def RestoreVolumeInfo(self):
		mplist = dm.maps()
		for mp in mplist:
			volume = self.mds.ReadVolumeInfo(mp.name)
			if not isinstance(volume, type(None)):
				self.MountVolume(volume)


def test():
	global PARAM
	client = Client(Mds_IP, int(Mds_Port))
	#client.ListVolume('hello_softsan_striped')

	# create a stiped type volume
	# arg = Object()
	# arg.type = 'striped'
	# arg.chunksizes = []
	# arg.subvolumes = []
	# arg.parameters = []
	# arg.name = 'hello_softsan_striped'
	# arg.size = 128
	# client.CreateVolume(arg)

	# create 2 linear type volumes and then 
	# build a striped volume with these two linear volumes

	# arg = Object()
	# arg.type = 'linear'
	# arg.chunksizes = []
	# arg.subvolumes = []
	# arg.parameters = []
	# arg.name = 'hello_softsan_1'
	# arg.size = 100
	# client.CreateVolume(arg)

	# arg = Object()
	# arg.type = 'linear'
	# arg.chunksizes = []
	# arg.subvolumes = []
	# arg.parameters = []
	# arg.name = 'hello_softsan_2'
	# arg.size = 100
	# client.CreateVolume(arg)

	# arg = Object()
	# arg.type = 'striped'
	# arg.chunksizes = []
	# arg.subvolumes = ['hello_softsan_1', 'hello_softsan_2']
	# arg.parameters = []
	# arg.name = 'hello_softsan_3'
	# arg.size = 200
	# client.CreateVolume(arg)
	# # client.DeleteVolume('hello_softsan_1')
	# # client.DeleteVolume('hello_softsan_2')
	# client.DeleteVolume('hello_softsan_3')

	# arg = Object()
	# arg.type = 'gfs'
	# arg.chunksizes = []
	# arg.subvolumes = []
	# arg.parameters = []
	# arg.name = 'gfs'
	# arg.size = 80
	# client.CreateVolume(arg)

	#client.DeleteVolume('gfs')
	client.Clear()


if __name__=='__main__':
	test()
