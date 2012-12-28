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

Client_IP = '192.168.0.12'
Mds_IP = '192.168.0.12'

Client_Port = 6789
Mds_Port = 1234


VolumeDict = {}

guid=msg.Guid()
guid.a=12; guid.b=13; guid.c=14; guid.d=15;
	
class ClientDeamon:
	
	def AssembleVolume(strategy, volumename = ''):
		tablist = []
		start = 0
		for segment in strategy:
			size = segment.size
			dmtype = segment.type
			if dmtype is 'striped':
				params = str(segment.snum) + ' ' + str(segment.stripesize) + ' '
			space = ''
			for chunk in segment.chunklist:
				params += space + chunk.path + ' ' + str(chunk.size)
				space = ' '
			table = dm.table(start, size, dmtype, params)
			tblist.append(table)
			strat += size
		dm.map(volumename, tblist)

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

	def MapVolume(self, req):
		volumename = req.volume.parameters[0]
		size = req.volume.size
		dmtype = req.volume.assembler

		if dmtype == 'linear':
			self.MapLinearVolume(volumename, req.volume.subvolumes)
		elif dmtype == 'striped':
			if len(req.volume.parameters) > 2:
				param = req.volume.parameters[2]
				stripedsize = int(req.volume.parameters[2])
			else:
				stripedsize = 256
			self.MapStripedVolume(volumename, stripedsize, req.volume.subvolumes)

		VolumeDict[volumename] = req.volume

		ret = msg.MapVolume_Response()
		ret.error = ''
		return ret

	def UnmapVolume(self, req):
		del VolumeDict[req.name]
		volume = VolumeDict[name]			
		for subvolume in volume.subvolumes:
			VolumeDict[subvolume.parameters[0]] = subvolume
		ret = msg.UnmapVolume_Response()
		ret.error = ''
		return ret

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
		if volume.assembler == 'chunk':
			del VolumeDict[name]
			return True
		for subvolume in volume.subvolumes:
			self.DeleteVolumeTree(subvolume)
		return True

	def RestoreVolumeInfo(self, volume = None):
		mplist = dm.maps()
		for mp in mplist:
			volume = self.ReadVolume(mp.name)
			if not volume == None:
				VolumeDict[mp.name] = volume

	def ReadVolumeInfo(self, name, addr=Mds_IP, port=Mds_Port):
		if not isinstance(name, str):
			path = Guid.toStr(name)
		with BuildStub(guid, addr, port, mds.MDS) as stub:
			req = msg.ReadVolume_Request()
			req.fullpath = '/'+path
			ret = stub.callMethod('ReadVolume', req)
		volume = msg.Volume()
		volume.ParseFromString(ret.volume)
		return volume

def test(server):
	mdsser = Object()
	mdsser.ServiceAddress = Mds_IP
	mdsser.ServicePort = Mds_Port
	serlist = server.GetChunkServers(mdsser)
	print serlist
	# chksize = 10
	# chksizes = [chksize]
	# server.NewChunkList(chksizes)

	# req = clmsg.NewVolume_Request()
	# req.volume_name = 'testlinear'
	# req.volume_size = 20
	# req.chunk_size.append(20)
	# server.NewVolume(req)

if __name__=='__main__':
	logging.basicConfig(level=logging.DEBUG)

	server=ClientDeamon()
	# test(server)
    #################### test read, write volume ###########################
	# server.WriteVolume(5120, 'linear')
	# print 'writevolume done'
	# server.ReadVolume()
     #################### test read, write volume ###########################

	# MI = {}
	# req = getattr(clmsg, 'NewVolume_Request', None)
	# res = getattr(clmsg, 'NewVolume_Response', type(None))
	# MI['NewVolume'] = (req, res)
	# req = getattr(clmsg, 'DeleteVolume_Request', None)
	# res = getattr(clmsg, 'DeleteVolume_Response', type(None))
	# MI['DeleteVolume'] = (req, res)
	# # print MI
	service=rpc.RpcService(server)
	framework=gevent.server.StreamServer(('0.0.0.0', Client_Port), service.handler)
	framework.serve_forever()