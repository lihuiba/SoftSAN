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
import util, config


Mds_IP = '192.168.0.12'
Mds_Port = 1234
Client_IP = '192.168.0.12'
Client_Port = 6789
CHUNKSIZE = 64
PARAM=None

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

	def Clear(self):
		self.socket.close()


class ChunkServerClient:
	
	def __init__(self, guid, csip, csport):
		#assert hasattr(ChunkServerClient, 'guid')
		self.guid = guid
		self.endpoint=(csip, csport)
	
	def getStub(self):
		if hasattr(self, 'stub'):
			return self.stub
		self.socket = gevent.socket.socket()
		self.socket.connect(self.endpoint)
		stub=rpc.RpcStub(self.guid, self.socket, ChunkServer.ChunkServer)
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
		# retrun value include access point of volume
		ret = stub.callMethod('DisassembleVolume', req)

	def Clear(self):
		self.socket.close()


class Client:
	def __init__(self, mdsip, mdsport):
		logging.basicConfig(level=logging.ERROR)

		self.guid = Guid.generate()
		self.chkpool = Pool(ChunkServerClient, ChunkServerClient.Clear)
		self.mds = MDSClient(self.guid, mdsip, mdsport)
		self.socket = gevent.socket.socket()
		self.socket.connect((Client_IP, Client_Port))
		self.stub = rpc.RpcStub(self.guid, self.socket, ClientDeamon.ClientDeamon)

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
		chkclient = self.chkpool.get(self.guid, addr, port)
		chkclient.DisassembleChunk(nodename)
		return errorinfo
		
	def MountVolume(self, volume):
		volume = self.mds.ReadVolumeInfo(volume)
		if volume.parameters[2] == 'active':
			logging.error('volume {0} is already mounted!'.fromat(volume.parameters[0]))
			return False
		req = msg.MountVolume_Request()
		obj2msg(volume, req.volume)
		self.stub.callMethod('MountVolume', req)
		if self.MountVolumeTree(volume) == True:
			volume.parameters[2] = 'inactive'
		return True

	def MountVolumeTree(self, volume):
		if volume.assembler == 'chunk':
			self.MountChunk(volume)
			return True
		for subvol in volume.subvolumes:
			self.MountVolumeTree(subvol)
		return True

	def UnmountVolume(self, volume):
		if isinstance(volume, str):
			volume = self.mds.ReadVolumeInfo(volume)
			if volume.parameters[2] == 'inactive':
				logging.error('volume {0} is already unmounted!'.fromat(volume.parameters[0]))
				return False
			req = msg.UnmountVolume_Request()
			req.name = volume.parameters[0]
			self.stub.callMethod('UnmountVolume', req)
		if volume.assembler == 'chunk':
			addr = volume.parameters[3]
			port = int(volume.parameters[4])
			chkclient = self.chkpool.get(self.guid, addr, port)
			if chkclient.UnmountChunk(volume) == False:
				return False
			return True
		for subvol in volume.subvolumes:
			self.UnmountVolume(subvol)
		volume.parameters[2] = 'inactive'
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
		volume = self.mds.ReadVolumeInfo(volumename)
		if volume.subvolumes[0].assembler == 'chunk':
			logging.error('This volume is not divisible!')
			return False

		req = msg.SplitVolume_Request()
		req.volume_name = volumename
		ret = self.stub.callMethod('SplitVolume', req)
		error = self.mds.DeleteVolumeInfo(volume)
		if error != '':
			logging.error(error)
			return False
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
				if vol.parameters[3] == 'used':
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
		volume.parameters = [volname, '/dev/mapper/'+volname, 'active', 'free']
		volume.parameters.extend(params)

		ret = self.MapVolume(volume)
		if ret == False:
			logging.error('Map volume failed')
			if volume.subvolumes[0].assembler == 'chunk':
				for subvol in volume.subvolumes:
					self.DeleteVolumeTree(subvol)
			return False

		self.mds.WriteVolumeInfo(volume)
		if len(volnames) > 0:
			for vol in vollist:
				vol.parameters[3] = 'used'
		for vol in vollist:
			self.WriteVolumeInfo(vol)
		return True
 
	def DeleteVolume(self, name):
		if isinstance(name, Object):
			name = name.parameters[0]
		self.DeleteVolumeTree(name)
		req = msg.ClientDeleteVolume_Request()
		req.volume_name = name
		ret = self.stub.callMethod('ClientDeleteVolume', req)

	def DeleteVolumeTree(self, volume):
		if isinstance(volume, str):
			volume = self.mds.ReadVolumeInfo(volume)
		self.mds.DeleteVolumeInfo(volume)
		if volume.assembler == 'chunk':
			addr = volume.parameters[3]
			port = int(volume.parameters[4])
			chkclient = self.chkpool.get(self.guid, addr, port)
			self.UnmountChunk(volume)
			chkclient.DeleteChunk(volume)
			return True
		for subvolume in volume.subvolumes:
			self.DeleteVolumeTree(subvolume)

	def InfoVolume(self, name):
		volume = self.mds.ReadVolumeInfo(name)
		space = '      '
		print 'name:', space, volume.parameters[0]
		print 'path:', space, volume.parameters[1]
		print 'size:', space, volume.size, 'MB'

	def RestoreVolumeInfo(self):
		mplist = dm.maps()
		for mp in mplist:
			volume = self.mds.ReadVolumeInfo(mp.name)
			if volume != None:
				req = msg.ClientWriteVolume_Request()
				obj2msg(volume, req.volume)
				self.stub.callMethod('ClientWriteVolume', req)

	def Clear(self):
		self.socket.close()
		self.mds.Clear()
		self.chkpool.dispose()


def test():
	global PARAM
	client = Client(PARAM.Mds_IP, int(PARAM.Mds_Port))
	client.InfoVolume('hello_softsan_striped')

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

def configuration():
	global PARAM
	helpmsg = '''group directories before files.
				augment with a --sort option, but any
				use of --sort=none (-U) disables grouping
			  '''
	default_cfgfile = './test.conf'

	cfgdict = (('MDS_IP', 'M', '192.168.0.149', 'ip address of metadata server'), \
				('MDS_PORT','m','6789','port of metadata server'), \
				('CHK_IP','C', '192.168.0.149', helpmsg), \
				('CHK_PORT','c', '3456', 'the port of chunk server'),\
				('enablexxx','x',False,'enable x'),\
				('cfgfile','f', default_cfgfile, 'name of the configuration file'))

	configure,_ = config.config(cfgdict)
	PARAM = util.Object(configure)
	default_cfgfile = './test.conf'
	print '----------------',PARAM.MDS_IP
	print '----------------',PARAM.MDS_PORT

def test_with_chkserv():
	global PARAM
	logging.basicConfig(level=logging.DEBUG)
	socket=gevent.socket.socket()
	socket.connect((PARAM.MDS_IP, int(PARAM.MDS_PORT)))
	guid=msg.Guid()
	guid.a=10
	guid.b=22
	guid.c=30
	guid.d=40
	stub=rpc.RpcStub(guid, socket, mds.MDS)
	arg=msg.GetChunkServers_Request()
	ret=stub.callMethod('GetChunkServers', arg)

	s2=gevent.socket.socket()
	s2.connect((ret.random[0].ServiceAddress, ret.random[0].ServicePort))
	stub2=rpc.RpcStub(guid, s2, ChunkServer.ChunkServer)

	if raw_input('you want to NewChunk ? ')=='y':
		arg2=msg.NewChunk_Request()
		arg2.size=32		
		arg2.count=2
		ret2=stub2.callMethod('NewChunk', arg2)
		print ret2.guids

	if raw_input('you want to assembleVolume ? ')=='y':
		req_assemble = msg.AssembleVolume_Request()
		req_assemble.volume.size = 32
		Guid.assign(req_assemble.volume.guid,ret2.guids[-1])
		res_assemble = stub2.callMethod('AssembleVolume', req_assemble)
		print 'access_point:', res_assemble.access_point

	if raw_input('you want to DisassembleVolume ? ')=='y':
		req_disassemble = msg.DisassembleVolume_Request()
		req_disassemble.access_point = res_assemble.access_point
		print 'req_disassemble.access_point:', req_disassemble.access_point
		stub2.callMethod('DisassembleVolume', req_disassemble)

	if raw_input('you want to DeleteChunk ? ')=='y':
		arg3=msg.DeleteChunk_Request()
		t = arg3.guids.add()
		Guid.assign(t, ret2.guids[0])
		t = arg3.guids.add()
		Guid.assign(t, ret2.guids[1])
		ret_Del=stub2.callMethod('DeleteChunk', arg3)

if __name__=='__main__':
	test_with_chkserv()
