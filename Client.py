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

CHUNKSIZE = 5120
VOLUMEPATH = '/Volume_DB2'

VolumeDictL = {}#Local volume dictionary
VolumeDictM = {}#Mds volume dictionary
volume_list = {}
guid=msg.Guid()
guid.a=12; guid.b=13; guid.c=14; guid.d=15;


def assignVolume(a, b):
	a.size = b.size
	a.assembler = b.assembler
	for param in b.parameters:
		a.parameters.append(param)
	for volume in b.subvolume:
		a.subvolume.append(volume)
	Guid.assign(a,guid, b.guid)

class BuildStub:
	def __init__(self, guid, server, interface):
		self.guid = guid
		self.server = server
		self.interface = interface
	def __enter__(self):
		self.socket = gevent.socket.socket()
		self.socket.connect((self.server.ServiceAddress, self.server.ServicePort))
		return rpc.RpcStub(self.guid, self.socket, self.interface)
	def __exit__(self, a, b, c):
		self.socket.close()
	
class Client:
	
	def MapVolumeMix(strategy, volumename = ''):
		tablist = []
		start = 0
		for segment in strategy:
			size = segment.size
			dmtype = segment.type
			if dmtpye is 'striped':
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
		pass

	def DeleteVolumeAll(self, volume):
		pass

	def DeleteVolume(self, req):
		volumename = req.volume_name
		print 'DeleteVolume:volumename is :    ' + volumename
		maplist = dm.maps()
		for mp in maplist:
			if mp.name == volumename:
				mp.remove()
				print 'DeleteleVolume:Disassemble successful'
				break

		print 'DeleteVolume:volume_list is :'
		print volume_list
		nodelist = volume_list[volumename][1]
		for node in nodelist:
			print 'node '+node.name+' is logging out'
			node.logout()
		del volume_list[volumename]
		print 'DeleteleVolume:Dismount nodes successful'

		res = clmsg.DeleteVolume_Response()
		res.result = 'successful'
		return res

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