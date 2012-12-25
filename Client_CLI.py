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
from Client import BuildStub


Mds_IP = '192.168.0.12'
Mds_Port = 1234
ChunkServer_IP = '192.168.0.12'
ChunkServer_Port = 3456
Client_IP = '192.168.0.12'
Client_Port = 6767

CHUNKSIZE = 5120

VolumeDictL = {}#Local volume dictionary
VolumeDictM = {}#Mds volume dictionary
VolumeDict = {}
guid=msg.Guid()
guid.a=12; guid.b=13; guid.c=14; guid.d=15;

class Client_CLI:

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

	def DeleteChunk(self, server, guids):
		with BuildStub(guid, server, ChunkServer.ChunkServer) as stub:
			arg = msg.DeleteChunk_Request()
			for chunkguid in guids:
				arg.guids.add()
				Guid.assign(arg.guids[-1], chunkguid)
			ret = stub.callMethod('DeleteChunk', arg)

	#give a list of chunk sizes, return a list of volumes
    #volume : path node msg.volume
	def NewChunkList(self, chksizes):
		volumelist = []
		Mds = Object()
		volume = Object()

		Mds.ServiceAddress = Mds_IP
		Mds.ServicePort = Mds_Port
		serlist = self.GetChunkServers(Mds)

		for size in chksizes:
			server = serlist.random[0]
			chunks = self.NewChunk(server, size, 1)
			if chunks == []:
				pass
			
			volume.server = server
			volume.size = size
			volume.assembler = 'chunk'
			volume.guid = msg.Guid()
			Guid.assign(volume.guid, chunks.guids[0])
			volume.path, volume.node = self.MountChunk(server, volume)

			#volumelist.append(volume)
			print volume.guid
			self.UnmountChunk(volumelist)
			if volume.node == None:
				self.UnmountChunk(volumelist)
				print 'Could not mount chunk'
				return None

			volumelist.append(volume)
			key = Guid.toTuple(volume.guid)
			VolumeDict[key] = volume

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

	def UnmountChunk(self, volumes):
		errorinfo = []#record error infomation
		for volume in volumes:
			volume.node.logout()
			with BuildStub(guid, volume.server, ChunkServer.ChunkServer) as stub:
				req = msg.DisassembleVolume_Request()
				req.access_point = volume.node.name
				ret = stub.callMethod('DisassembleVolume', req)
			
			guids = [volume.guid]
			self.DeleteChunk(volume.server, guids)
		return errorinfo
		
	def NewVolume(self, req):
		vollist = []
		volume = Object()

		volname = req.volume_name
		if volname in VolumeDict:
			print 'volume name has been used! find another one'
		volsize = req.volume_size
		voltype = req.volume_type
		chksizes = req.chunk_size
		volnames = req.subvolume
		params = req.params

		if len(volnames) > 0:
			for name in volnames:
				if not name in VolumeDict:
					pass
				vollist.append(VolumeDict[name])
		
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

		with BuildStub(guid, server, Client.Client) as stub:
			req = msg.MapVolume_Request()
			req.volumename = volname
			req.type = voltype
			req.params = params
			for vol in vollist:
				req.path.append(vol.path)
				req.size.append(vol.size)
			ret = stub.callMethod('MapVolume', req)

		#volume = util.message2object(ret.volume)
		volume.size = volsize
		volume.assembler = voltype
		volume.parameters.append(params)
		volume.guid = Guid.generate()
		for vol in vollist:
			pass

		mvolume = msg.Volume()
		util.object2message(volume, mvolume)
		self.WriteVolume(mvolume)

		key = Guid.toTuple(volume.guid)
		VolumeDict[key] = volume
		VolumeDict[volname] = volume

	def ListVolume(self):
		maplist = dm.maps()
		for mp in maplist:
			print mp.name

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
		
		volume = msg.Volume()
		volume.ParseFromString(ret.volume)

		return volume

	def MoveVolume(self, source, dest):
		with BuildStub(guid, server, mds.MDS) as stub:
			req = msg.MoveVolume_Request()
			req.source = source
			req.destination = dest
			ret = stub.callMethod('MoveVolume', req)

def test(server):
	server.ListVolume()

	chksizes = [10]
	server.NewChunkList(chksizes)
	

if __name__=='__main__':
	server = Client_CLI()
	test(server)