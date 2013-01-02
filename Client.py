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
import ClientDeamon
from util import message2object as msg2obj
from util import object2message as obj2msg
from argparse import ArgumentParser as ArgParser


Mds_IP = '192.168.0.12'
Mds_Port = 1234
Client_IP = '192.168.0.12'
Client_Port = 6789

CHUNKSIZE = 12

LVNAME='lv_softsan_'

guid=msg.Guid()
guid.a=12; guid.b=13; guid.c=14; guid.d=15;

class SocketPool:
	def __init__(self):
		self.pool = {}
	def getConnection(self, endpoint):
		if endpoint in self.pool:
			return self.pool[endpoint]
		socket = gevent.socket.socket()
		socket.connect(endpoint)
		self.pool[endpoint] = socket
		return socket
	def closeAllConnection(self):
		for endpoint in self.pool:
			socket = self.pool[endpoint]
			del self.pool[endpoint]
			socket.close

pool = SocketPool()

def ParseArg():
	ArgsDict={'create':['c',  '',       'create a volume'],
			  'table' :['t',  sys.stdin,'volume construction table'],
			  'remove':['rm', '',		'remove a volume'],
			  'list'  :['l',  '',		'list object information'],
			  'unmap' :['',   '',       'split a volume into subvolumes']
	}
	cfgfile = 'test.conf'



class Client:

	def GetChunkServers(self, addr, port, count = 5):
		socket = pool.getConnection((addr, port))
		stub = rpc.RpcStub(guid, socket, mds.MDS)
		arg = msg.GetChunkServers_Request()
		arg.randomCount = count
		serverlist = stub.callMethod('GetChunkServers', arg)
		return serverlist

	def NewChunk(self, addr, port, size, count = 1):
		socket = pool.getConnection((addr, port))
		stub = rpc.RpcStub(guid, socket, ChunkServer.ChunkServer)
		arg = msg.NewChunk_Request()
		arg.size = size
		arg.count = count
		chunklist = stub.callMethod('NewChunk', arg)
		return chunklist

	def DeleteChunk(self, addr, port, volumes):
		socket = pool.getConnection((addr, port))
		stub = rpc.RpcStub(guid, socket, ChunkServer.ChunkServer)
		arg = msg.DeleteChunk_Request()
		if isinstance(volumes, list) == True:
			for volume in volumes:
				arg.guids.add()
				volguid = Guid.fromStr(volume.guid)
				Guid.assign(arg.guids[-1], volguid)
		else:
			arg.guids.add()
			volguid = Guid.fromStr(volumes.guid)
			Guid.assign(arg.guids[-1], volguid)
		ret = stub.callMethod('DeleteChunk', arg)

	#give a list of chunk sizes, return a list of volumes
    #volume : path node msg.volume
	def NewChunkList(self, chksizes):
		volumelist = []
		
		serlist = self.GetChunkServers(Mds_IP, Mds_Port)

		for size in chksizes:
			server = serlist.random[0]
			addr = server.ServiceAddress
			port = server.ServicePort
			chunks = self.NewChunk(addr, port, size, 1)
			if chunks == []:
				pass
			
			print 'chunksize: ', size
			volume = Object()
			volume.size = size
			volume.assembler = 'chunk'
			volume.guid = Guid.toStr(chunks.guids[0])
			path, nodename = self.MountChunk(addr, port, volume)
			volume.parameters = [volume.guid, path, nodename,
			server.ServiceAddress, server.ServicePort]

			if path == None:
				self.UnmountChunk(volumelist)
				print 'Could not mount chunk'
				return None

			volumelist.append(volume)
		return volumelist

	def AssembleChunk(self, addr, port, volume):
		socket = pool.getConnection((addr, port))
		stub = rpc.RpcStub(guid, socket, ChunkServer.ChunkServer)
		req = msg.AssembleVolume_Request()
		obj2msg(volume, req.volume)
		target = stub.callMethod('AssembleVolume', req)
		return target

	def DisassembleChunk(self, addr, port, nodename):
		socket = pool.getConnection((addr, port))
		stub = rpc.RpcStub(guid, socket, ChunkServer.ChunkServer)
		req = msg.DisassembleVolume_Request()
		req.access_point = nodename
		ret = stub.callMethod('DisassembleVolume', req)

	def GetChunkNode(self, name, addr, port=3260):
		nodelist = libiscsi.discover_sendtargets(addr, port)
		for node in nodelist:
			if name == node.name:
				return node
		return None

	def MountChunk(self, addr, port, volume):
		target = self.AssembleChunk(addr, port, volume)
		node = self.GetChunkNode(target.access_point, addr)
		if not node == None:
			node.login()
			dev = scandev.get_blockdev_by_targetname(node.name)
			if not dev == None:
				return dev, node.name
		return None, None

	def UnmountChunk(self, volumes):
		print 'removing chunks......'
		if not isinstance(volumes, list):
			volumes = [volumes]
		errorinfo = []#record error information
		for volume in volumes:
			addr = volume.parameters[3]
			port = int(volume.parameters[4])
			nodename = volume.parameters[2]
			print 'nodename: ', nodename
			node = self.GetChunkNode(nodename, addr)
			if not node == None:
				node.logout()
			self.DisassembleChunk(addr, port, nodename)
		return errorinfo
		
	def MountVolume(self, volume):
		if volume.assembler == 'chunk':
			addr = volume.parameters[3]
			port = volume.parameters[4]
			self.MountChunk(addr, port, voluem)
			return True
		for subvol in volume.subvolumes:
			self.MountVolume(subvol)
		return True

	def UnmountVolume(self):
		if volume.assembler == 'chunk':
			addr = volume.parameters[3]
			port = volume.parameters[4]
			self.UnmountChunk(addr, port, voluem)
			return True
		for subvol in volume.subvolumes:
			self.UnmountVolume(subvol)
		return True

	def MapVolume(self, volume, addr=Client_IP, port=Client_Port):
		socket = pool.getConnection((addr, port))
		stub = rpc.RpcStub(guid, socket, ClientDeamon.ClientDeamon)
		req = msg.MapVolume_Request()
		obj2msg(volume, req.volume)
		ret = stub.callMethod('MapVolume', req)
		if ret.error == '':
			return True
		return False

	def UnmapVolume(self, volumename, addr=Client_IP, port=Client_Port):
		socket = pool.getConnection((addr, port))
		stub = rpc.RpcStub(guid, socket, ClientDeamon.ClientDeamon)
		req = msg.UnmapVolume_Request()
		req.name = volumename
		ret = stub.callMethod('UnmapVolume', req)

		volume = self.ReadVolumeInfo(volumename)
		for subvol in volume.subvolumes:
			self.WriteVolumeInfo(subvol)
		self.DeleteVolumeInfo(volume)

	def NewVolume(self, arg):
		vollist = []
		volume = Object()

		volname = arg.name
		if volname in VolumeDict:
			print 'volume name has been used! find another one'
		volsize = arg.size
		voltype = arg.type
		chksizes = arg.chunksizes
		volnames = arg.subvolumes
		params = arg.params

		if len(volnames) > 0:
			for name in volnames:
				ret = self.ReadVolumeInfo(name)
				vol = msg2obj(ret)
				if vol == None:
					pass
				vollist.append(vol)
		
		if voltype == '':
			voltype = 'linear'
		if len(chksizes) == 0 and len(volnames) == 0:
			totsize = volsize
			while totsize > CHUNKSIZE:
				chksizes.append(CHUNKSIZE)
				totsize -= CHUNKSIZE
			chksizes.append(totsize)

		if len(chksizes) > 0:
			vollist = self.NewChunkList(chksizes)
			if vollist == None:
				pass
			for vol in vollist:
				print 'vol.size: ', vol.size
				msgvol = msg.Volume()
				obj2msg(vol, msgvol)
				self.WriteVolumeInfo(msgvol)

		volume.size = volsize
		volume.assembler = voltype
		volume.subvolumes = vollist
		volume.guid = Guid.toStr(Guid.generate())
		volume.parameters = [volname, '/dev/mapper/'+volname]
		volume.parameters.extend(params)
		ret = self.MapVolume(volume)
		if ret == None:
			self.UnmountChunk(vollist)
			pass
		msgvolume = msg.Volume()
		obj2msg(volume, msgvolume)
		self.WriteVolumeInfo(msgvolume)

	def DeleteVolume(self, name):
		self.DeleteVolumeTree(name)
		self.DeleteVolumeInfo(name)
		socket = pool.getConnection((addr, port))
		stub = rpc.RpcStub(guid, socket, ClientDeamon.ClientDeamon)
		req = msg.ClientDeleteVolume_Request()
		req.name = name
		ret = stub.callMethod('ClientDeleteVolume', req)

	def DeleteVolumeTree(self, volume):
		if isinstance(volume, str):
			volume = self.ReadVolumeInfo(volume)
		if volume.assembler == 'chunk':
			self.UnmountChunk(volume)
			return True
		mps = dm.maps()
		name = volume.parameters[0]
		for mp in mps:
			if name == mp.name:
				mp.remove()
		for subvolume in volume.subvolumes:
			self.DeleteVolumeTree(subvolume)

	def ListVolume(self):
		maplist = dm.maps()
		for mp in maplist:
			print mp.name

	def WriteVolumeInfo(self, volume, addr=Mds_IP, port=Mds_Port):
		if isinstance(volume, str) == True:
			path = volume
		else:
			path = volume.parameters[0]
		socket = pool.getConnection((addr, port))
		stub = rpc.RpcStub(guid, socket, mds.MDS)
		req = msg.WriteVolume_Request()
		req.volume = volume.SerializeToString()
		req.fullpath = '/'+path
		ret = stub.callMethod('WriteVolume', req)

	def DeleteVolumeInfo(self, volume, addr=Mds_IP, port=Mds_Port):
		if isinstance(volume, str) == True:
			path = volume
		else:
			path = volume.parameters[0]
		socket = pool.getConnection((addr, port))
		stub = rpc.RpcStub(guid, socket, mds.MDS)
		req = msg.DeleteVolume_Request()
		req.fullpath = '/'+path
		ret = stub.callMethod('DeleteVolume', req)

	def ReadVolumeInfo(self, name, addr=Mds_IP, port=Mds_Port):
		if isinstance(name, str) == True:
			path = name
		else:
			path = Guid.toStr(name)
		socket = pool.getConnection((addr, port))
		stub = rpc.RpcStub(guid, socket, mds.MDS)
		req = msg.ReadVolume_Request()
		req.fullpath = '/'+path
		ret = stub.callMethod('ReadVolume', req)
		volume = msg.Volume()
		volume.ParseFromString(ret.volume)
		return volume

	def MoveVolumeInfo(self, source, dest, addr=Mds_IP, port=Mds_Port):
		socket = pool.getConnection((addr, port))
		stub = rpc.RpcStub(guid, socket, mds.MDS)
		req = msg.MoveVolume_Request()
		req.source = source
		req.destination = dest
		ret = stub.callMethod('MoveVolume', req)

def test(server):
	serlist = server.GetChunkServers(Mds_IP, Mds_Port)
	print serlist
	# server.ListVolume()

	# chksizes = [10]
	# chklist = server.NewChunkList(chksizes)
	# print len(chklist)

	# arg = Object()
	# arg.type = 'striped'
	# arg.chunksizes = []
	# arg.subvolumes = []
	# arg.params = ''
	# arg.name = 'hello_softsan'
	# arg.size = 24
	# server.NewVolume(arg)

	# print volume.size
	# server.DeleteVolume('hello_softsan')
	
	
if __name__=='__main__':
	server = Client()
	test(server)