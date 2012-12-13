import rpc, logging
import messages_pb2 as msg
import mds, ChkSvr_mock
import gevent.socket
import guid as Guid
import block.dm as dm
import mds.Object as Object

nodelist = []

class Client:
	def GetChunkServers(guid, MdsIP, SerPort, count = 3):
		socket = gevent.socket.socket()
		socket.connect(MdsIP, SerPort)
		stub = rpc.RpcStub(guid, socket, mds.MDS)
		arg = msg.GetChunkServers_Request()
		arg.randomCount = count
		serverlist = stub.callMethod('GetChunkServers', arg)
		return serverlist

	def NewChunk(guid, serverlist, size, count = 1):
		socket = gevent.socket.socket()
		server = socket.random(serverlist)
		socker.connect(server.ServiceAddress, server.ServicePort)
		stub = RpcStub(guid, socket, ChkSvr_mock.ChunkServer)
		arg = msg.NewChunk_Request()
		arg.size = size
		arg.count = count
		chunklist = stub.callMethod('NewChunk', arg)
		return chunklist

	def DeleteChunk():
		pass

	def MountChunk(iqn):
		print 'iqn is :' + iqn
		nodelist = libiscsi.discover_sendtargets(iqn)
		nodelist[0].login()
		print 'nodelist: ---' + nodelist[0].name
		blkdev = scandev.get_blockdev_by_targetname(nodelist[0].name)
		return blkdev

	def DismountChunk(iqn):
		for node in nodelist:
			if iqn is node.name:
				node.logout()
				nodelist.remove( node )
				break

	# def AddLinearDevice(table, start, dev):
	# 	table += str(start) + ' ' + str(dev.size) + ' linear ' + str(dev.path) + ' 0\n'
	# 	return table, start+dev.size

	# def AddStripedDevices(table, start, devlist, stripesize = 2048):#1mb
	# 	num = len(devlist)
	# 	totsize = 0
	# 	for dev in devlist:
	# 		totsize += dev.size
	# 	table += str(start) + '' + str(totsize) + ' striped ' + str(num) + ' ' + str(stripesize)
	# 	for dev in devlist:
	# 		table += ' ' + dev.path + ' 0'
	# 	table += '\n'
	# 	return table, start+totsize

	# def CreateMappingTable(devlist, type = 'linear', stripesize = 2048):
	# 	table = ''
	# 	start = 0
	# 	if tpye is 'linear':
	# 		for dev in devlist:
	# 			ret = AddLinearDevice(table, start, dev)
	# 			table = ret[0]
	# 			start = ret[1]
	# 	if tpye is 'stripe':
	# 		AddStripedDevices(table, start, devlist, stripesize)
	# 	path = os.getcwd() + 'dmtable'
	# 	fp = open(path, w)
	# 	fp.write(table)
	# 	return path

	#def BuildStrategy(size, dmtype, chunklist = None, stripesize = 256):

	def AssembleVolume(volumename = None, strategy):
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

	def AssembleLinearVolume(volumename, chunklist):
		tblist = []
		start = 0
		for chunk in chunklist:
			size = chunk.size
			dmtype = 'linear'
			params = chunk.path
			table = dm.table(start, size, dmtype, params)
			tblist.append(table)
			start += size
		dm.map(volumename, tblist)

	def AssembleStripedVolume(volumename, chunklist):
		tblist = []
		start = 0
		size = chunk.size
		dmtype = 'striped'
		params = ''
		space = ''
		for chunk in chunklist:
			params += space + chunk.path
			space = ' '
		table = dm.table(start, size, dmtype, params)
		tblist.append(table)
		dm.map(volumename, tblist)

	def NewVolume(self, req):#not finished
		volname = req.name
		volsize = req.size
		voltpye = req.type
		chklist = req.chklist
		strsize = req.strsize
		if voltype = None:
			voltype = 'linear'
		if chklist is None:
			pass
		if strsize is None:
			strsize = 256
		chknum = len(chklist)

	def DisassembleVolume(volumename):
		maplist = dm.maps()
		for mp in maplist:
			if mp.name is volumename:
				mp.remove()
				break

if __name__=='__main__':
	guid=msg.Guid()
	guid.a=12; guid.b=13; guid.c=14; guid.d=15;
	logging.basicConfig(level=logging.DEBUG)

	server=Client()
	service=rpc.RpcService(server)
	framework=gevent.server.StreamServer(('0.0.0.0', PORT), service.handler)
	framework.serve_forever()