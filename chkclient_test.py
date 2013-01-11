import Client
import rpc, logging
import messages_pb2 as msg
import mds, ChunkServer
import gevent.socket
import guid as Guid
import block.dm as dm
from mds import Object
import libiscsi
import scandev

def configuration():
	global PARAM
	helpmsg = '''group directories before files.
				augment with a --sort option, but any
				use of --sort=none (-U) disables grouping
			  '''
	default_cfgfile = './test.conf'

	cfgdict = (('MDS_IP', 'M', '192.168.0.149', 'ip address of metadata server'), \
				('MDS_PORT','m','6789','port of metadata server'), \
				('CHK_IP','C', '192.168.0.149', helpmsg), \
				('CHK_PORT','c', '3456', 'the port of chunk server'),\
				('enablexxx','x',False,'enable x'),\
				('cfgfile','f', default_cfgfile, 'name of the configuration file'))

	configure,_ = config.config(cfgdict)
	PARAM = util.Object(configure)
	default_cfgfile = './test.conf'
	print '----------------',PARAM.MDS_IP
	print '----------------',PARAM.MDS_PORT


def test_with_chkserv():
	global PARAM

	# logging.basicConfig(level=logging.DEBUG)
	# socket=gevent.socket.socket()
	# socket.connect((PARAM.MDS_IP, int(PARAM.MDS_PORT)))
	# guid=msg.Guid()
	# guid.a=10
	# guid.b=22
	# guid.c=30
	# guid.d=40
	# stub=rpc.RpcStub(guid, socket, mds.MDS)
	# arg=msg.GetChunkServers_Request()
	# ret=stub.callMethod('GetChunkServers', arg)

	# s2=gevent.socket.socket()
	# s2.connect((ret.random[0].ServiceAddress, ret.random[0].ServicePort))
	# stub2=rpc.RpcStub(guid, s2, ChunkServer.ChunkServer)

	client = Client(PARAM.MDS_IP, int(PARAM.MDS_PORT))
	serlist = client.mds.GetChunkServers()
	print len(serlist)
	print serlist[0].ServiceAddress, serlist[0].ServicePort

	chkaddr = serlist[0].ServiceAddress
	chkport = serlist[0].ServicePort
	chkclient = ChunkServerClient(client.guid, chkaddr, chkport)
	chklist = chkclient.NewChunk(32, 1)
	print chklist


	volume = Object()
	volume.size = 50
	volume.guid = Guid.toStr(chklist.guids[0])
	target = chkclient.AssembleChunk(volume)
	print target

	chkclient.DisassembleChunk(target.access_point)

	chkclient.DeleteChunk(volume)

if __name__=='__main__':
	configuration()
	test_with_chkserv()