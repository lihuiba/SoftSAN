import rpc, logging
import messages_pb2 as msg
import client_messages_pb2 as clmsg
import mds, ChkSvr_mock
import gevent.socket
import guid as Guid
import block.dm as dm
from mds import Object
import libiscsi
import scandev

PORT = 6767
CHUNKSIZE = 5120

volume_list = {}
guid=msg.Guid()
guid.a=12; guid.b=13; guid.c=14; guid.d=15;

class Client:
	
	def GetChunkServers(self, MdsIP, SerPort, count = 5):
		socket = gevent.socket.socket()
		socket.connect((MdsIP, SerPort))
		stub = rpc.RpcStub(guid, socket, mds.MDS)
		arg = msg.GetChunkServers_Request()
		arg.randomCount = count
		serverlist = stub.callMethod('GetChunkServers', arg)
		return serverlist

	def NewChunk(self, server, size, count = 1):
		socket = gevent.socket.socket()
		socket.connect((server.ServiceAddress, server.ServicePort))
		stub = rpc.RpcStub(guid, socket, ChkSvr_mock.ChunkServer)
		arg = msg.NewChunk_Request()
		arg.size = size
		arg.count = count
		# print 'Client_test: arg :'
		# print arg
		chunklist = stub.callMethod('NewChunk', arg)
		return chunklist
		#print chunklist

	def NewChunkList(self, chksizes):
		serlist = self.GetChunkServers('192.168.0.12', 2345)
		# print 'Client_test: serlist = '
		# print serlist
		chklist = []
		nodelist = []
		server = serlist.random[0]
		# print 'Client_test: chunksizes = '
		# print chksizes
		chunks = self.NewChunk(server, chksizes[0], 2)

		ret = self.MountChunk(Guid.toStr(chunks.guids[0]), '192.168.0.12')
		chunk = Object()
		chunk.size = chksizes[0]
		chunk.path = ret[0]
		chklist.append(chunk)
		nodelist.append(ret[1])

		ret = self.MountChunk(Guid.toStr(chunks.guids[1]), '192.168.0.12')
		chunk2 = Object()
		chunk2.size = chksizes[0]
		chunk2.path = ret[0]
		chklist.append(chunk2)
		nodelist.append(ret[1])
		# print 'Client: chklist = '
		# for chunk in chklist:
		# 	print chunk.size
		# 	print chunk.path

		# for size in chksizes:
		# 	server = serlist.random[0]
		# 	chunks = NewChunk(guid, server, size, 1)
		# 	chunk = Object()
		# 	chunk.size = size
		# 	chunk.path = MountChunk(Guid.toStr(chunks.guids[0]), '192.168.0.12')
		# 	chklist.add(chunk)
		return chklist, nodelist

	def MountChunk(self, iqn, addr, port = 3260):
		iqn = 'iqn:'+iqn
		nodelist = libiscsi.discover_sendtargets(addr, port)
		for node in nodelist:
			#print node.name
			if iqn == node.name:
				node.login()
				blkdev = scandev.get_blockdev_by_targetname(iqn)
				return blkdev, node
		return 'No found!', None

	def DismountChunk(iqn, nodelist):
		iqn = 'iqn:'+iqn
		for node in nodelist:
			if iqn == node.name:
				node.logout()
				nodelist.remove( node )
				break

	def AssembleVolume(strategy, volumename = ''):
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

	def AssembleLinearVolume(self, volumename, chunklist):
		tblist = []
		start = 0
		for chunk in chunklist:
			size = chunk.size
			params = chunk.path + ' 0'
			table = dm.table(start, size, 'linear', params)
			tblist.append(table)
			start += size
		print volumename
		for table in tblist:
		 	print table.start; print table.size; print table.params;
		dm.map(volumename, tblist)

	def AssembleStripedVolume(self, volumename, strsize, chunklist):
		tblist = []
		start = 0
		size = 0
		dmtype = 'striped'
		params = str( len(chunklist) ) + ' ' + str( strsize )
		for chunk in chunklist:
			size += chunk.size
			params += ' ' + chunk.path + ' 0'
		print params
		table = dm.table(start, size, dmtype, params)
		tblist.append(table)
		dm.map(volumename, tblist)

	def NewVolume(self, req):
		volname = req.volume_name
		volsize = req.volume_size
		voltype = req.volume_type
		chksizes = req.chunk_size
		strsize = req.striped_size
		if voltype is '':
			voltype = 'linear'
		if len(chksizes) is 0:
			while volsize > CHUNKSIZE:
				chksizes.append(CHUNKSIZE)
				volsize -= CHUNKSIZE
			chksizes.append(volsize)

		ret = self.NewChunkList(chksizes)
		chklist = ret[0]
		volume_list[volname] = ret[1]
		# print 'Client:NewVolume:volume_list:'
		# for name in volume_list:
		# 	print name
		# 	for node in volume_list[name]:
		# 		print node
		if voltype == 'linear':
		 	self.AssembleLinearVolume(volname, chklist)
		else:
		  	if strsize is 0:
		  		strsize = 256
		  	self.AssembleStripedVolume(volname, strsize, chklist)

		ret = clmsg.NewVolume_Response()
		ret.name = volname
		ret.size = volsize
		return ret

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
		nodelist = volume_list[volumename]
		for node in nodelist:
			print 'node '+node.name+' is logging out'
			node.logout()
		del volume_list[volumename]
		print 'DeleteleVolume:Dismount nodes successful'

		res = clmsg.DeleteVolume_Response()
		res.result = 'successful'
		return res

if __name__=='__main__':
	logging.basicConfig(level=logging.DEBUG)

	server=Client()
	MI = {}
	req = getattr(clmsg, 'NewVolume_Request', None)
	res = getattr(clmsg, 'NewVolume_Response', type(None))
	MI['NewVolume'] = (req, res)
	req = getattr(clmsg, 'DeleteVolume_Request', None)
	res = getattr(clmsg, 'DeleteVolume_Response', type(None))
	MI['DeleteVolume'] = (req, res)
	print MI
	service=rpc.RpcService(server, MI)
	framework=gevent.server.StreamServer(('0.0.0.0', PORT), service.handler)
	framework.serve_forever()