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
from argparse import ArgumentParser as ArgParser
from collections import Iterable


Mds_IP = '192.168.0.12'
Mds_Port = 1234
Client_IP = '192.168.0.12'
Client_Port = 6789

CHUNKSIZE = 64

LVNAME='lv_softsan_'

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

def ParseLine(words):
	arg = Object()
	arg.name = ''
	arg.size = 0
	arg.type = ''
	arg.chunksizes = []
	arg.subvolumes = []
	arg.parameters = []
	tableFormat = 'name size [type] [num] [sizes|volumenames]'
	num = len(words)

	if num < 2:
		return None
	if isinstance(words[0], str)==False or isinstance(words[1], int)==False:
		print tableFormat
		return None
	arg.name = words[0]
	arg.size = words[1]

	if num >= 3:
		if words[2].isdigit() == False:
			if words[2] == 'striped':
				arg.type = 'striped'
			elif words[2] == 'linear':				
				arg.type = 'linear'
			elif words[2] == 'gfs':
				arg.type = 'gfs'
			else:
				error = 'Unrecognized argument: ' + words[2]
		#else:
			

	if isinstance(word[3], int):
		totsize = 0
		for i in range(3, len(words)):
			if not isinstance(words[i], int):
				print tableFormat
				return arg
			totsize += words[i]
			arg.chunksizes.append(words[i])
		remain = words[1]-totsize
		if(remain < 0):
			print 'chunk size summary is not equal to volume size'
			return arg
		while remain > CHUNKSIZE:
			arg.chunksizes.append(CHUNKSIZE)
			remain -= CHUNKSIZE
		if remain > 0:
			arg.chunksizes.append(remain)

	if isinstance(word[3], str):
		for i in range(3, len(words)):
			if not isinstance(words[i], str):
				print tableFormat
				return arg
			arg.subvolumes.append(words[i])

	return arg

def CheckName(client, name):
	if name == '':
		return ''
	if not isinstance(name, str):
		return  'Name should be a string!'
	if client.ReadVolumeInfo(name) == None:
		return 'Volume ' + name + ' is not exsit!'
	return ''

def ParseArgs(client):
	parser = ArgParser()
	subparsers = parser.add_subparsers()
	# create
	parser_create = subparsers.add_parser('create', help='create a volume')
	parser_create.add_argument('table', nargs='+', help='table to build a volume')
	parser_create.add_argument('--file', dest='path', help='table file')
	#parser_create.add_argument('--num', '-n', nargs='*', dest='subvolumes', help='chunk or subvolume')
	parser_create.set_defaults(func='Create')
	# remove
	parser_remove = subparsers.add_parser('remove', help='remove a volume')
	parser_remove.add_argument('volume_name', help='volume to be removed')
	parser_create.set_defaults(func='Delete')
	# list
	parser_list = subparsers.add_parser('list', help='show volume info')
	parser_list.add_argument('volume_name', help='volume to be removed')
	parser_create.set_defaults(func='List')
	# split
	parser_split = subparsers.add_parser('split', help='split a volume')
	parser_split.add_argument('volume_name', help='volume to be removed')
	parser_create.set_defaults(func='Split')
	# mount
	parser_mount = subparsers.add_parser('mount', help='mount a volume')
	parser_mount.add_argument('volume_name', help='volume to be removed')
	parser_create.set_defaults(func='Mount')
	# unmount
	parser_unmount = subparsers.add_parser('unmount', help='unmount a volume')
	parser_unmount.add_argument('volume_name', help='volume to be removed')
	parser_create.set_defaults(func='Unmount')

	args = parser.parse_args()
	if args.func == 'Create':
		data = ParseLine(args.table)
	else:
		data = args.volume_name
		error = CheckName(data)
		if not error == '':
			print error
			return None
		
	getattr(client, args.func+'Volume')(data)


# def ParseFile(path):
# 	fp = open(path)
# 	lines = fp.readlines()
# 	for line in lines:
# 		ParseLine(line)

# def ParseArg(client):
# 	ArgsDict={'create' :['c',  '',       'create a volume'],
# 			  'table'  :['t',  sys.stdin,'volume construction table'],
# 			  'remove' :['rm', '',		 'remove a volume'],
# 			  'list'   :['l',  '',		 'list object information'],
# 			  'unmap'  :['',   '',       'split a volume into subvolumes'],
# 			  'mount'  :['',   '',       'Mount a exist volume'],
# 			  'unmount':['',   '',		 'Unmount a volume']
# 	}
# 	ArgsFile = 'test.conf'
# 	args = Object(config.config(ArgsDict, ArgsFile))

# 	print args.create

	# if hasattr(args, 'create'):
	# 	print args.create
	# 	#arg = ParseLine(args.create)
	# 	#print arg.name, arg.size, arg.type, arg.chksizes
	# 	#client.create(arg)
	# if hasattr(args, 'remove'):
	# 	name = args.remove
	# 	client.remove(name)
	# if hasattr(args, 'list'):
	# 	name = args.list
	# 	client.list(name)
	# if hasattr(args, 'unmap'):
	# 	name = args.unmap
	# 	client.unmap(name)
	# if hasattr(args, 'mount'):
	# 	name = args.mount
	# 	client.mount(name)
	# if hasattr(args, 'unmount'):
	# 	name = args.unmount
	# 	client.unmount(name)

class MDSClient:
	def __init__(self, guid, mdsip, mdsport):
		socket = gevent.socket.socket()
		socket.connect((mdsip, mdsport))
		self.stub = rpc.RpcStub(guid, socket, mds.MDS)
	
	def GetChunkServers(self, count=None):
		stub = self.getStub_MDS()
		arg = msg.GetChunkServers_Request()
		if count:
			arg.randomCount = count
		resp = stub.callMethod('GetChunkServers', arg)
		serverlist=[message2object(x) for x in resp.random]
		return serverlist

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
		try:
			ret = stub.callMethod('ReadVolume', req)
			volume = msg.Volume()
			volume.ParseFromString(ret.volume)
			return volume
		except:
			return None

	def MoveVolumeInfo(self, source, dest, addr=Mds_IP, port=Mds_Port):
		socket = pool.getConnection((addr, port))
		stub = rpc.RpcStub(guid, socket, mds.MDS)
		req = msg.MoveVolume_Request()
		req.source = source
		req.destination = dest
		ret = stub.callMethod('MoveVolume', req)



class ChunkServerClient:
	# class-level members
	pool=SocketPool()
	MI=rpc.BuildMethodInfo(ChunkServer.ChunkServer)
	
	def __init(self, csip, csport):
		assert hasattr(ChunkServerClient, 'guid')
		self.endpoint=(csip, csport)
	
	def getStub(self):
		if hasattr(self, 'stub'):
			return self.stub
		socket=self.pool.getConnection(self.endpoint)
		stub=rpc.RpcStub(guid, socket, self.MI)
		self.stub=stub
		return stub

	def NewChunk(self, size, count = 1):
		arg = msg.NewChunk_Request()
		arg.size = size
		arg.count = count
		stub = self.getStub()
		chunklist = stub.callMethod('NewChunk', arg)
		return chunklist

	def NewChunk(self, stub, size, count = 1):
		arg = msg.NewChunk_Request()
		arg.size = size
		arg.count = count
		chunklist = stub.callMethod('NewChunk', arg)
		return chunklist

	def DeleteChunk(self, stub, volumes):
		arg = msg.DeleteChunk_Request()
		if not isinstance(volumes, Iterable):
			volumes=(volumes,)
		for volume in volumes:
			t=arg.guids.add()
			volguid = Guid.fromStr(volume.guid)
			Guid.assign(t, volguid)
		ret = stub.callMethod('DeleteChunk', arg)

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


class Client:
	def __init__(self, mdsip, mdsport):
		self.guid=Guid.generate()
		ChunkServerClient.guid=self.guid
		self.pool=SocketPool()

	def getStub_ChunkServer(self, ChunkServerIP, ChunkServerPort):
		socket = pool.getConnection((ChunkServerIP, ChunkServerPort))
		stub = rpc.RpcStub(self.guid, socket, self.csmi)
		return stub

	def GetChunkServers(self, count):
		stub = self.getStub_MDS()
		arg = msg.GetChunkServers_Request()
		arg.randomCount = count
		serverlist = stub.callMethod('GetChunkServers', arg)
		return serverlist

	#give a list of chunk sizes, return a list of volumes
    #volume : path node msg.volume
	def NewChunkList(self, chksizes):
		volumelist = []
		
		serlist = self.GetChunkServers()

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

	def UnmountVolume(self, volume):
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

	def SplitVolume(self, volumename, addr=Client_IP, port=Client_Port):
		socket = pool.getConnection((addr, port))
		stub = rpc.RpcStub(guid, socket, ClientDeamon.ClientDeamon)
		req = msg.UnmapVolume_Request()
		req.name = volumename
		ret = stub.callMethod('UnmapVolume', req)

		volume = self.ReadVolumeInfo(volumename)
		for subvol in volume.subvolumes:
			self.WriteVolumeInfo(subvol)
		self.DeleteVolumeInfo(volume)

	def CreateVolume(self, arg):
		vollist = []
		volume = Object()

		volname = arg.name
		if volname in VolumeDict:
			print 'volume name has been used! find another one'
		volsize = arg.size
		voltype = arg.type
		chksizes = arg.chunksizes
		volnames = arg.subvolumes
		params = arg.parameters

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
	# server.CreateVolume(arg)

	# print volume.size
	# server.DeleteVolume('hello_softsan')
	
	
if __name__=='__main__':
	server = Client()
	#test(server)
	#ParseArgs(server)
	server.ReadVolumeInfo('hello')