import rpc, logging
import messages_pb2 as msg
import mds, ChunkServer
import gevent.socket
import guid as Guid
import block.dm as dm
from mds import Object
import libiscsi
import scandev

Client_IP = '192.168.0.12'
Mds_IP = '192.168.0.12'
Client_Port = 6789
Mds_Port = 1234

VolumeDict = {}

class ClientDeamon:

	def MapLinearVolume(self, volumename, devlist):
		tblist = []
		start = 0
		for dev in devlist:
			size = dev.size*2048
			params = dev.parameters[1] + ' 0'
			table = dm.table(start, size, 'linear', params)
			tblist.append(table)
			start += size
		try:
			dm.map(volumename, tblist)
		except:
			return False
		return True

	def MapStripedVolume(self, volumename, strsize, devlist):
		tblist = []
		start = 0
		size = 0
		params = str( len(devlist) ) + ' ' + str( strsize )
		for dev in devlist:
			size += dev.size*2048
			params += ' ' + dev.parameters[1] + ' 0'
		print params
		table = dm.table(start, size, 'striped', params)
		tblist.append(table)
		try:
			dm.map(volumename, tblist)
		except:
			return False
		return True

	def MapGFSVolume(self, volumename, devlist):
		tblist = []
		start = 0
		num = len(devlist)
		i = 0
		while i < num:
			params = 'core 2 64 nosync 3'
			params += ' '+devlist[i].parameters[1]+' 0'
			params += ' '+devlist[i+1].parameters[1]+' 0'
			params += ' '+devlist[i+2].parameters[1]+' 0'
			size = devlist[i].size*2048
			table = dm.table(start, size, 'mirror', params)
			tblist.append(table)
			start += size
			i += 3
		try:
			dm.map(volumename, tblist)
		except:
			return False
		return True

	def MapVolume(self, req):		
		volumename = req.volume.parameters[0]
		size = req.volume.size
		dmtype = req.volume.assembler

		logging.debug('Mapping volume:', req.volume.parameters[0])

		if dmtype == 'linear':
			result = self.MapLinearVolume(volumename, req.volume.subvolumes)
		elif dmtype == 'striped':
			if len(req.volume.parameters) > 2:
				stripedsize = int(req.volume.parameters[2])
			else:
				stripedsize = 256
			result = self.MapStripedVolume(volumename, stripedsize, req.volume.subvolumes)
		elif dmtype == 'gfs':
			result = self.MapGFSVolume(volumename, req.volume.subvolumes)

		ret = msg.MapVolume_Response()
		if result == False:
			ret.error = 'device mapper: Mapping volume failed'
			logging.error(ret.error)
			return ret

		VolumeDict[volumename] = req.volume
		ret.error = ''
		return ret

	def SplitVolume(self, req):
		del VolumeDict[req.name]
		maps = dm.maps()
		for mp in maps:
			if mp.name == req.name:
				mp.remove()
		ret = msg.UnmapVolume_Response()
		ret.error = ''
		return ret

	def MountVolume(self, volume):
		if not isinstance(volume, Object):
			volume = msg2obj(volume.volume)
		if volume.assembler == 'chunk':
			return True
		for vol in subvolumes:
			self.MountVolume(vol)
		req = Object()
		req.volume = volume
		self.MapVolume(req)

	def UnmountVolume(self, req):
		self.DeleteVolumeTree(req.name)

	def ClientWriteVolume(self, req):
		volume = msg2obj[req.volume]
		VolumeDict[req.name] = volume

	def ClientDeleteVolume(self, req):
		self.DeleteVolumeTree(req.name)
		ret = msg.ClientDeleteVolume_Response()
		ret.error = ''
		return ret

	def DeleteVolumeTree(self, name):
		if name in VolumeDict:
			volume = VolumeDict[name]
		else:
			return False
		del VolumeDict[name]
		if volume.assembler == 'chunk':
			del VolumeDict[name]
			return True
		mps = dm.maps()
		name = volume.parameters[0]
		for mp in mps:
			if name == mp.name:
				mp.remove()
		for subvolume in volume.subvolumes:
			self.DeleteVolumeTree(subvolume)
		return True


if __name__=='__main__':
	logging.basicConfig(level=logging.DEBUG)

	server=ClientDeamon()
	service=rpc.RpcService(server)
	framework=gevent.server.StreamServer(('0.0.0.0', Client_Port), service.handler)
	framework.serve_forever()