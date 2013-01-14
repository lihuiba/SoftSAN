import rpc, logging
import messages_pb2 as msg
import mds, ChunkServer
import gevent.socket
import guid as Guid
import block.dm as dm
from mds import Object
import libiscsi
import scandev

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
		except Exception as ex:
			logging.error(str(ex))
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
		except Exception as ex:
			logging.error(str(ex))
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
		except Exception as ex:
			logging.error(str(ex))
			return False
		return True

	def MapVolume(self, req):		
		volumename = req.volume.parameters[0]
		size = req.volume.size
		dmtype = req.volume.assembler

		logging.info('Mapping volume:', req.volume.parameters[0])

		if dmtype == 'linear':
			result = self.MapLinearVolume(volumename, req.volume.subvolumes)
		elif dmtype == 'striped':
			if len(req.volume.parameters) > 3:
				stripedsize = int(req.volume.parameters[3])
			else:
				stripedsize = 256
			result = self.MapStripedVolume(volumename, stripedsize, req.volume.subvolumes)
		elif dmtype == 'gfs':
			result = self.MapGFSVolume(volumename, req.volume.subvolumes)

		ret = msg.MapVolume_Response()
		ret.error = ''
		if result == False:
			ret.error = 'device mapper: map volume {0} failed'.format(volumename)
			logging.error(ret.error)
			return ret

		VolumeDict[volumename] = req.volume
		for subvol in req.volume.subvolumes:
			VolumeDict[subvol.parameters[0]] = subvol
		return ret

	def SplitVolume(self, req):
		ret = msg.UnmapVolume_Response()
		ret.error = ''
		try:
			maps = dm.maps()
			for mp in maps:
				if mp.name == req.volume_name:
					mp.remove()
		except Exception as ex:
			logging.error('device mapper: split volume {0} failed'.format(req.volume_name))
			logging.error(str(ex))
		del VolumeDict[req.volume_name]
		return ret

	def MountVolume(self, req):
		volname = req.volume.parameters[0]
		ret = msg.MountVolume_Response()
		ret.error = ''
		if volname in VolumeDict:
			ret.error = 'volume {0} is already mounted!'.fromat(volname)
			return ret
		if self.MountVolumeTree(req.volume) == False:
			ret.error = 'mount volume '+volname+' failed'
			return ret
		VolumeDict[volname] = req.volume
		return ret

	def MountVolumeTree(self, volume):
		volname = volume.parameters[0]
		if volume.assembler == 'chunk':
			VolumeDict[volname] = volume
			return True
		for vol in volume.subvolumes:
			if self.MountVolumeTree(vol) == False:
				return False
		VolumeDict[volname] = volume
		req = Object()
		req.volume = volume
		self.MapVolume(req)
		return True

	def UnmountVolume(self, req):
		ret = msg.UnmountVolume_Response()
		ret.error = ''
		if not req.volume_name in VolumeDict:
			ret.error = 'volume {0} is already unmounted!'.format(req.volume_name)
			return ret
		if self.DeleteVolumeTree(req.volume_name) == False:
			ret.error = 'unmount volume {0} failed'.format(req.volume_name)
		return ret

	def ClientWriteVolume(self, req):
		volume = msg2obj(req.volume)
		VolumeDict[req.name] = volume
		ret = msg.ClientWriteVolume_Response()
		ret.error = ''
		return ret

	def ClientDeleteVolume(self, req):
		ret = msg.ClientDeleteVolume_Response()
		ret.error = ''
		if self.DeleteVolumeTree(req.volume_name) == False:
			ret.error = 'delete volume {0} failed'.format(req.volume_name)		
		return ret

	def DeleteVolumeTree(self, volume):
		if volume in VolumeDict:
			volume = VolumeDict[volume]
		else:
			return False
		if volume.assembler == 'chunk':
			del VolumeDict[volume.parameters[0]]
			return True
		try:
			mps = dm.maps()
			name = volume.parameters[0]
			for mp in mps:
				if name == mp.name:
					mp.remove()
		except Exception as ex:
			logging.error('device mapper: removing volume {0} failed'.format(volume.parameters[0]))
			logging.error(str(ex))
			return False
		for subvolume in volume.subvolumes:
			if self.DeleteVolumeTree(subvolume.parameters[0]) == False:
				return False
		del VolumeDict[volume.parameters[0]]
		return True


if __name__=='__main__':
	logging.basicConfig(level=logging.DEBUG)

	server=ClientDeamon()
	service=rpc.RpcService(server)
	framework=gevent.server.StreamServer(('0.0.0.0', 6767), service.handler)
	framework.serve_forever()