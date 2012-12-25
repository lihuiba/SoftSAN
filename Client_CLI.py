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

	def DismountChunk(iqn, nodelist):
		for node in nodelist:
			if iqn == node.name:
				node.logout()
				nodelist.remove( node )
				break

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

if __name__=='__main__':
	server = Client_CLI()
	test(server)