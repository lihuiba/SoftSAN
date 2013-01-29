import rpc, logging
import messages_pb2 as msg
import mds, ChunkServer
import gevent.socket
import guid as Guid
import block.dm as dm
from mds import Object
import libiscsi
import scandev


class DMClient:

	def MapLinearVolume(self, volumename, devlist):
		tblist = []
		start = 0
		for dev in devlist:
			size = dev.size*2048
			params = dev.parameters[1] + ' 0'
			table = dm.table(start, size, 'linear', params)
			tblist.append(table)
			start += size
		dm.map(volumename, tblist)
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
		dm.map(volumename, tblist)
		return True

	def MapMirrorVolume(self, volumename, devlist):
		tblist = []
		start = 0
		params = 'core 2 64 nosync ' + len(devlist)
		for dev in devlist:
			size += dev.size*2048
			params += ' ' + dev.parameters[1] + ' 0'
		table = dm.table(start, size, 'mirror', params)
		tblist.append(table)
		dm.map(volumename, tblist)
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
		dm.map(volumename, tblist)
		return True

	def MapVolume(self, volume):		
		volumename = volume.parameters[0]
		size = volume.size
		dmtype = volume.assembler

		if dmtype == 'linear':
			result = self.MapLinearVolume(volumename, volume.subvolumes)
		elif dmtype == 'striped':
			if len(volume.parameters) > 4:
				stripedsize = int(volume.parameters[4])
			else:
				stripedsize = 256
			result = self.MapStripedVolume(volumename, stripedsize, volume.subvolumes)
		elif dmtype == 'mirror':
			result = self.MapMirrorVolume(volumename, volume.subvolumes)
		elif dmtype == 'gfs':
			result = self.MapGFSVolume(volumename, volume.subvolumes)

		return result

	def SplitVolume(self, volume_name):
		mp = self.GetVolumeMap(volume_name)
		if mp == None:
			logging.error('device mapper: could not find volume map')
			return False
		mp.remove()
		return True

	def MountVolume(self, volume):
		if volume.assembler == 'chunk':
			return True
		for subvol in volume.subvolumes:
			if self.MountVolume(subvol) == False:
				return False
		if self.MapVolume(volume) == False:
			return False
		return True

	def DeleteVolume(self, volume):
		if volume.assembler == 'chunk':
			return True
		mp = self.GetVolumeMap(volume.parameters[0])
		if mp == None:
			logging.error('device mapper: could not find volume map')
			return False
		mp.remove()
		for subvolume in volume.subvolumes:
			if self.DeleteVolume(subvolume) == False:
				return False
		return True

	def GetVolumeMap(self, name):
		mps = dm.maps()
		for mp in mps:
			if name == mp.name:
				return mp
		return None