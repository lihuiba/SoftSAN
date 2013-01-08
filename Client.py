import rpc, logging, sys
import messages_pb2 as msg
import client_messages_pb2 as clmsg
import mds, ChunkServer
import gevent.socket
import guid as Guid
import block.dm as dm
from mds import Object
import libiscsi
import scandev, config
import ClientDeamon
from util import message2object as msg2obj
from util import object2message as obj2msg
from util import Pool
from collections import Iterable


Mds_IP = '192.168.0.12'
Mds_Port = 1234
Client_IP = '192.168.0.12'
Client_Port = 6789
CHUNKSIZE = 64


def createSocket(endpoint):
	socket = gevent.socket.socket()
	socket.connect(endpoint)
	return socket

def closeSocket(socket):
	socket.close()


class MDSClient:
	def __init__(self, guid, mdsip, mdsport):
		socket = gevent.socket.socket()
		socket.connect((mdsip, mdsport))
		self.stub = rpc.RpcStub(guid, socket, mds.MDS)
	
	def GetChunkServers(self, count=None):
		arg = msg.GetChunkServers_Request()
		if count:
			arg.randomCount = count
		ret = self.stub.callMethod('GetChunkServers', arg)
		serverlist=[msg2obj(x) for x in ret.random]
		return serverlist

	def WriteVolumeInfo(self, volume):
		if isinstance(volume, str):
			path = volume
		else:
			path = volume.parameters[0]
		req = msg.WriteVolume_Request()
		req.volume = volume.SerializeToString()
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
			volume = msg.Volume()
			volume.ParseFromString(ret.volume)
			return msg2obj(volume)
		except:
			return None

	def MoveVolumeInfo(self, source, dest):
		req = msg.MoveVolume_Request()
		req.source = source
		req.destination = dest
		ret = self.stub.callMethod('MoveVolume', req)


class ChunkServerClient:
	# class-level members
	pool = Pool(createSocket, closeSocket)
	MI=rpc.BuildMethodInfo(ChunkServer.ChunkServer)
	
	def __init__(self, guid, csip, csport):
		#assert hasattr(ChunkServerClient, 'guid')
		self.guid = guid
		self.endpoint=(csip, csport)
	
	def getStub(self):
		if hasattr(self, 'stub'):
			return self.stub
		socket=self.pool.get(self.endpoint)
		stub=rpc.RpcStub(self.guid, socket, ChunkServer.ChunkServer)
		self.stub=stub
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
		stub = self.getStub()
		req = msg.AssembleVolume_Request()
		obj2msg(volume, req.volume)
		target = stub.callMethod('AssembleVolume', req)
		return target

	def DisassembleChunk(self, nodename):
		stub = self.getStub()
		req = msg.DisassembleVolume_Request()
		req.access_point = nodename
		ret = stub.callMethod('DisassembleVolume', req)

	def Clear():
		self.pool.dispose()


class Client:
	def __init__(self, mdsip, mdsport):
		self.guid = Guid.generate()
		self.pool = Pool(ChunkServerClient)
		self.mds = MDSClient(self.guid, mdsip, mdsport)
		socket = gevent.socket.socket()
		socket.connect((Client_IP, Client_Port))
		self.stub = rpc.RpcStub(self.guid, socket, ClientDeamon.ClientDeamon)

	# give a list of chunk sizes, return a list of volumes
    # volume : path node msg.volume
	def NewChunkList(self, chksizes):
		volumelist = []
		
		serlist = self.mds.GetChunkServers()

		for size in chksizes:
			server = serlist[0]
			addr = server.ServiceAddress
			port = server.ServicePort
			chkclient = self.pool.get(self.guid, addr, port)
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
				print 'Could not mount chunk'
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
		chkclient = self.pool.get(self.guid, addr, port)
		target = chkclient.AssembleChunk(volume)
		node = self.GetChunkNode(target.access_point, addr)
		if not node == None:
			node.login()
			dev = scandev.get_blockdev_by_targetname(node.name)
			if not dev == None:
				return dev, node.name
		return None, None

	def UnmountChunk(self, volume):
		errorinfo = []#record error information
		addr = volume.parameters[3]
		port = int(volume.parameters[4])
		nodename = volume.parameters[2]
		node = self.GetChunkNode(nodename, addr)
		if node != None:
			node.logout()
		chkclient = self.pool.get(self.guid, addr, port)
		chkclient.DisassembleChunk(nodename)
		return errorinfo
		
	def MountVolume(self, volume):
		if volume.assembler == 'chunk':
			self.MountChunk(volume)
			return True
		for subvol in volume.subvolumes:
			self.MountVolume(subvol)
		return True

	def UnmountVolume(self, volume):
		if volume.assembler == 'chunk':
			addr = volume.parameters[3]
			port = int(volume.parameters[4])
			chkclient = self.pool.get(self.guid, addr, port)
			if chkclient.UnmountChunk(volume) == False:
				return False
			return True
		for subvol in volume.subvolumes:
			self.UnmountVolume(subvol)
		return True

	def MapVolume(self, volume):
		req = msg.MapVolume_Request()
		obj2msg(volume, req.volume)
		ret = self.stub.callMethod('MapVolume', req)
		if ret.error == '':
			return True
		print ret.error
		return False

	def SplitVolume(self, volumename):
		req = msg.SplitVolume_Request()
		req.name = volumename
		ret = self.stub.callMethod('SplitVolume', req)

		volume = self.mds.ReadVolumeInfo(volumename)
		self.mds.DeleteVolumeInfo(volume)

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
				if vol == None:
					pass
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
				print 'New Chunk failed'
				pass
			for vol in vollist:
				print 'vol.size: ', vol.size
				msgvol = msg.Volume()
				obj2msg(vol, msgvol)
				self.mds.WriteVolumeInfo(msgvol)

		volume.size = volsize
		volume.assembler = voltype
		volume.subvolumes = vollist
		volume.guid = Guid.toStr(Guid.generate())
		volume.parameters = [volname, '/dev/mapper/'+volname]
		volume.parameters.extend(params)

		ret = self.MapVolume(volume)
		if ret == False:
			if volume.subvolumes[0].assembler == 'chunk':
				for subvol in volume.subvolumes:
					self.DeleteVolumeTree(subvol)
			return False

		msgvolume = msg.Volume()
		obj2msg(volume, msgvolume)
		self.mds.WriteVolumeInfo(msgvolume)
		return True
 
	def DeleteVolume(self, name):
		if isinstance(name, Object):
			name = name.parameters[0]
		self.DeleteVolumeTree(name)
		req = msg.ClientDeleteVolume_Request()
		req.name = name
		ret = self.stub.callMethod('ClientDeleteVolume', req)

	def DeleteVolumeTree(self, volume):
		if isinstance(volume, str):
			volume = self.mds.ReadVolumeInfo(volume)
		self.mds.DeleteVolumeInfo(volume)
		if volume.assembler == 'chunk':
			addr = volume.parameters[3]
			port = int(volume.parameters[4])
			chkclient = self.pool.get(self.guid, addr, port)
			self.UnmountChunk(volume)
			chkclient.DeleteChunk(volume)
			return True
		mps = dm.maps()
		name = volume.parameters[0]
		for mp in mps:
			if name == mp.name:
				mp.remove()
		for subvolume in volume.subvolumes:
			self.DeleteVolumeTree(subvolume)

	def ListVolume(self, name):
		volume = self.mds.ReadVolumeInfo(name)
		print volume.parameters[0]
		print volume.size

	def RestoreVolumeInfo(self):
		mplist = dm.maps()
		for mp in mplist:
			volume = self.mds.ReadVolumeInfo(mp.name)
			if volume != None:
				req = msg.ClientWriteVolume_Request()
				obj2msg(volume, req.volume)
				self.stub.callMethod('ClientWriteVolume', req)


def test():
	client = Client(Mds_IP, Mds_Port)

	arg = Object()
	arg.type = 'striped'
	arg.chunksizes = []
	arg.subvolumes = []
	arg.parameters = []
	arg.name = 'hello_softsan_striped'
	arg.size = 128
	client.CreateVolume(arg)

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
	
if __name__=='__main__':
	test()