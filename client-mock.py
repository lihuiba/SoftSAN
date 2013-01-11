import rpc, logging, config, util
import messages_pb2 as msg
import ChunkServer
import gevent.socket
import mds
import guid as Guid

# MDS_IP='192.168.0.149'
# MDS_PORT=2340
# CHK_IP='192.168.0.149'
# CHK_PORT=6780
PARAM=None



# socket.close()
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


def test():
	global PARAM
	logging.basicConfig(level=logging.DEBUG)
	socket=gevent.socket.socket()
	socket.connect((PARAM.MDS_IP, int(PARAM.MDS_PORT)))
	guid=msg.Guid()
	guid.a=10
	guid.b=22
	guid.c=30
	guid.d=40
	stub=rpc.RpcStub(guid, socket, mds.MDS)
	arg=msg.GetChunkServers_Request()
	ret=stub.callMethod('GetChunkServers', arg)

	s2=gevent.socket.socket()
	s2.connect((ret.random[0].ServiceAddress, ret.random[0].ServicePort))
	stub2=rpc.RpcStub(guid, s2, ChunkServer.ChunkServer)

	if raw_input('you want to NewChunk ? ')=='y':
		arg2=msg.NewChunk_Request()
		arg2.size=32		
		arg2.count=2
		ret2=stub2.callMethod('NewChunk', arg2)
		print ret2.guids

	if raw_input('you want to assembleVolume ? ')=='y':
		req_assemble = msg.AssembleVolume_Request()
		req_assemble.volume.size = 32
		Guid.assign(req_assemble.volume.guid,ret2.guids[-1])
		res_assemble = stub2.callMethod('AssembleVolume', req_assemble)
		print 'access_point:', res_assemble.access_point

	if raw_input('you want to DisassembleVolume ? ')=='y':
		req_disassemble = msg.DisassembleVolume_Request()
		req_disassemble.access_point = res_assemble.access_point
		print 'req_disassemble.access_point:', req_disassemble.access_point
		stub2.callMethod('DisassembleVolume', req_disassemble)

	if raw_input('you want to DeleteChunk ? ')=='y':
		arg3=msg.DeleteChunk_Request()
		t = arg3.guids.add()
		Guid.assign(t, ret2.guids[0])
		t = arg3.guids.add()
		Guid.assign(t, ret2.guids[1])
		ret_Del=stub2.callMethod('DeleteChunk', arg3)
	

if __name__ == '__main__':
	configuration()
	test()