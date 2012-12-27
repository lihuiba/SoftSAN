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
ChunkServer_IP = '192.168.0.12'
Client_Port = 6767
Mds_Port = 2340
ChunkServer_Port = 6780

VolumeDict = {}

guid=msg.Guid()
guid.a=12; guid.b=13; guid.c=14; guid.d=15;

class BuildStub:
	def __init__(self, guid, address, port, interface):
		self.guid = guid
		self.addr = address
		self.port = port
		self.interface = interface
	def __enter__(self):
		self.socket = gevent.socket.socket()
		self.socket.connect((self.addr, self.port))
		return rpc.RpcStub(self.guid, self.socket, self.interface)
	def __exit__(self, a, b, c):
		self.socket.close()
	
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
			size = dev.size
			params = dev.path + ' 0'
			table = dm.table(start, size, 'linear', params)
			tblist.append(table)
			start += size
		# print volumename
		# for table in tblist:
		#  	print table.start; print table.size; print table.params;
		dm.map(volumename, tblist)

	def MapStripedVolume(self, volumename, strsize, devlist):
		tblist = []
		start = 0
		size = 0
		params = str( len(devlist) ) + ' ' + str( strsize )
		for dev in devlist:
			size += dev.size
			params += ' ' + dev.path + ' 0'
		print params
		table = dm.table(start, size, 'striped', params)
		tblist.append(table)
		dm.map(volumename, tblist)

	def MapVolume(self, req):
		volumename = req.volumename
		size = req.size
		dmtype = req.type
		params = req.params
		devlist = []
		dev = Object()
		for i < len(req.size):
			dev.size = req.size[i]
			dev.path = req.path[i]

		if dmtype == 'linear':
			self.MapLinearVolume(volumename, devlist)
		elif dmtype == 'striped':
			stripedsize = int(params)
			self.MapStripedVolume(volumename, devlist)

		volume.size = volsize
		volume.assembler = voltype
		volume.parameters = []
		volume.parameters.append(volname)
		volume.parameters.append(params)
		volume.guid = Guid.generate()
		volume.subvolume = []
		for vol in vollist:
			volume.subvolume.append(vol)

		mvolume = msg.Volume()
		util.object2message(volume, mvolume)
		self.WriteVolume(mvolume)

		key = Guid.toTuple(volume.guid)
		VolumeDict[key] = volume
		VolumeDict[volname] = volume

	def DeleteVolume(self, arg = None):
		if arg == None:
			print 'Please specify a volume by name or its guid'
		if isinstance(arg, str):
			key = arg
		elif:
			key = Guid.toTuple(guid)

		if not key in VolumeDict:
			print 'No such volume found'

		volume = VolumeDict[key]

		if volume.assembler == 'chunk':
			addr = volume.parameters[0]
			port = int(volume.parameters[1])
			volumes = [volume]
			error = self.UnmountChunk(volumes)
			return error

		for subvolume in volume.subvolume:
			error = DelVolume(subvolume.guid)

		mplist = dm.maps()
		for mp in maplist:
			if volume.name == mp.name:
				mp.remove()

		self.DeleteVolume(self, Mds_IP, Mds_Port, volume.name)
		key = Guid.toTuple(volume.guid)
		del VolumeDict[key]

	def RestoreVolumeInfo(self, volume = None):
		#self.ReadVolume()
		mplist = dm.maps()
		for mp in mplist:
			retvolume = self.ReadVolume(Mds_IP, Mds_Port, mp.name)
			if not retvolume == None:
				volume = util.message2object(retvolume)
				if volume.assembler == 'chunk':
					chunkname = LVNAME+Guid.toStr(volume.guid)
					addr = volume.parameters[0]
					#port = volume.parameters[1]
					nodelist = libiscsi.discover_sendtargets(addr, 3260)
					for node in nodelist:
						if chunkname == node.name:
							volume.node = node
							break
					key = Guid.toTuple(volume.guid)
					VolumeDict[key] = volume

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

	server=Client()
	test(server)
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
	# service=rpc.RpcService(server, MI)
	# framework=gevent.server.StreamServer(('0.0.0.0', Client_Port), service.handler)
	# framework.serve_forever()