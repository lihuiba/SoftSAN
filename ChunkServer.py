import rpc, logging
import messages_pb2 as msg
import guid as Guid
import mds
import gevent.server
import Backend
from pytgt.tgt_ctrl import *
import random

MDS_IP=None
MDS_PORT=None
CHK_IP=None
CHK_PORT=None
VGNAME=None
LVNAME='lv_softsan_'

class ChunkServer:
	def __init__(self):
		self.lvm = Backend.LVM_SOFTSAN()
		self.tgt = Tgt()
	
#  always use lun_index=1. 
	def AssembleVolume(self, req):
		self.tgt.reload()
		ret = msg.AssembleVolume_Response()
		str_guid = Guid.toStr(req.volume.guid)
		lv_name = LVNAME+str_guid
		if not self.lvm.haslv(lv_name):
			ret.error = "Chunk {0} does not exist!".format(str_guid)
			return ret
		target_name = "iqn:softsan_"+str_guid
		target_id = self.tgt.target_name2target_id(target_name)
		if target_id != None:
			ret.access_point = target_name
			return ret
		while True:
			target_id = str(random.randint(0,1024*1024))
			if not self.tgt.is_in_targetlist(target_id): 
				break			
		lun_path = '/dev/'+VGNAME+'/'+lv_name
		if self.tgt.new_target_lun(target_id, target_name, lun_path, 'ALL')!=None:
			ret.error = "Failed to export chunk {0} with tgt".format(str_guid)
			return ret
		ret.access_point = target_name
		return ret
		
	def DisassembleVolume(self, req):
		self.tgt.reload()
		ret = msg.DisassembleVolume_Response()
		target_name = req.access_point
		target_id = self.tgt.target_name2target_id(target_name)
		if target_id==None:
			ret.error='No such access_point'
			return ret
		if self.tgt.delete_target(target_id)!=None:
			ret.error=('failed to Disassemble Volume'+target_name)
		return ret

# try to create every requested chunk.however, if some chunk can not be created, fill the ret.error with the output of lvcreate 
	def NewChunk(self, req):
		self.lvm.reload()
		ret = msg.NewChunk_Response()
		size = str(req.size)+'M'
		for i in range(req.count):
			a_guid = Guid.generate()
			lv_name = LVNAME+Guid.toStr(a_guid)
			lv_size = size
			output =  self.lvm.lv_create(VGNAME, lv_name, lv_size)
			if output!=None:
				ret.error = str(i) + ':' + output + ' '
				break
			t=ret.guids.add()
			Guid.assign(t, a_guid)
		return ret

# try to delete every requested chunk. if it can not delete, fill the ret.error with output of lvremove
	def DeleteChunk(self, req):
		self.lvm.reload()
		ret = msg.DeleteChunk_Response()
		for a_guid in req.guids:
			str_guid=Guid.toStr(a_guid)
			lv_name = LVNAME+str_guid
			lv_path = '/dev/'+VGNAME+'/'+lv_name
			output = self.lvm.lv_remove(lv_path)
			if output!=None:
				ret.error = "Unable to delete chunk {0}:\n{1}".format(str_guid, output)
				break
			t=ret.guids.add()
			Guid.assign(t, a_guid)
		return ret

def doHeartBeat(server, stub, socket):
	global LVNAME
	chunkserver_ip=socket.getsockname()[0]
	while True:
		info=msg.ChunkServerInfo()
		info.ServiceAddress=chunkserver_ip
		info.ServicePort=CHK_PORT
		server.lvm.reload_softsan_lvs()
		for lv in server.lvm.softsan_lvs:
			chk=info.chunks.add()
			name4guid = lv.name.split(LVNAME)[1]
			Guid.assign(chk.guid, Guid.fromStr(name4guid))
			chk.size = int(lv.get_sizes(lv.total_extents)[2])
		stub.callMethod('ChunkServerInfo', info)
		print 'for test', random.randint(0,100),'________________________________________________________'
		gevent.sleep(1)

def heartBeat(server):
	guid=Guid.generate()
	stub=rpc.RpcStub(guid, None, mds.MDS)
	while True:
		try:
			socket=gevent.socket.socket()
			out = socket.connect((MDS_IP, MDS_PORT))
			stub.socket=socket
			doHeartBeat(server, stub, socket)
		except KeyboardInterrupt:
			print "asdf9asdfasdfasdf^C"
			raise
		except:
			logging.error('An error occured during heart beat, preparing to retry', exc_info=1)
			gevent.sleep(2)

def test_ChunkServer():
	print '     test begin     '.center(100,'-')
	print
	# mock the newchunk request from client
	req_newchunk=msg.NewChunk_Request()
	req_newchunk.size=32
	req_newchunk.count=1
	ret_newchunk = server.NewChunk(req_newchunk)
	# mock the assemblevolume request from client
	req_assemblevolume=msg.AssembleVolume_Request()
	Guid.assign(req_assemblevolume.volume.guid, ret_newchunk.guids[-1])
	req_assemblevolume.volume.size=32
	ret_assemblevolume = server.AssembleVolume(req_assemblevolume)
	# mock req_disassemblevolume
	req_disassemblevolume = msg.DisassembleVolume_Response()
	req_disassemblevolume.access_point = ret_assemblevolume.access_point
	ret_disassemblevolume = server.DisassembleVolume(req_disassemblevolume)
	print ret_disassemblevolume.access_point
	# mock the delchunk request from client
	req_delchunk = msg.DeleteChunk_Request()
 	for a_guid in ret_newchunk.guids:
		t=req_delchunk.guids.add()
		Guid.assign(t, a_guid)
	ret_delchunk = server.DeleteChunk(req_delchunk)
	print
	print '     test end     '.center(100,'-')

def usage():
	print 'Welcome to SoftSAN 0.1,  ChunkServer usage...'
	print 'python ChunkServer.py -a -b -c -d -n -h'
	print '-a ip of meatadata server(192.168.0.149) '
	print '-b port of meatadata server(1234) '
	print '-c ip of chunkserver(192.168.0.149)'
	print '-d port of chunkserver(5678)'
	print '-n name of VolGroup '

def config_from_cmd():
	global MDS_IP, MDS_PORT, CHK_IP, CHK_PORT, VGNAME
	import getopt, sys
	verbose = ["mdsip=", "mdsport=", "chksvrip=", "chksvrport=", "vgname=", "verbose", "help"]
	abbrev = "a:b:c:d:n:v:h"
	try:
		opts, args = getopt.getopt(sys.argv[1:], abbrev, verbose)
	except getopt.GetoptError, err:
		print str(err) # will print something like "option -a not recognized"
		usage()
		sys.exit(2)
	mdsip=None
	mdsport=None
	chksvrip=None
	chksvrport=None
	vgname=None
	for o, a in opts:
		if o == ("-v", "--verbose"):
			print 'SoftSAN 0.1 ......'
			sys.exit()
		elif o in ("-h", "--help"):
			usage()
			sys.exit()
		elif o in ("-a", "--mdsip"):
			mdsip = a
		elif o in ("-b", "--mdsport"):
			mdsport = a
		elif o in ("-c", "--chksvrip"):
			chksvrip = a
		elif o in ("-d", "--chksvrport"):
			chksvrport = a
		elif o in ("-n", "--name"):
			vgname = a
		else:
			assert False, "unhandled option"
	if mdsip != None:
		MDS_IP=mdsip
	if mdsport != None:
		MDS_PORT = int(mdsport)
	if chksvrip != None:
		CHK_IP = chksvrip
	if chksvrport != None:
		CHK_PORT = int(chksvrport)
	if vgname != None:
		VGNAME = vgname
	print 'config from command >>> ', 'mdsip:', MDS_IP, 'mdsport:',MDS_PORT, 'vgname:',VGNAME, 'chksvrip:', CHK_IP, 'chksvrport:', CHK_PORT

def config_from_file(filename='/home/hanggao/SoftSAN/test.conf'):
	global MDS_IP, MDS_PORT, CHK_IP, CHK_PORT, VGNAME
	import ConfigParser
	config = ConfigParser.ConfigParser()
	config.readfp(open(filename,'rb'))
	if MDS_IP==None:
		try:
			mdsip = config.get('global', 'mdsip')
			MDS_IP = mdsip
		except:
			pass
	if MDS_PORT==None:
		try:
			mdsport = config.get('global', 'mdsport')
			MDS_PORT = int(mdsport)
		except:
			pass
	if VGNAME==None:
		try:
			vgname = config.get('global', 'vgname')
			VGNAME = vgname
		except:
			pass
	if CHK_IP==None:
		try:
			chksvrip = config.get('global', 'chksvrip')
			CHK_IP = chksvrip
		except:
			pass
	if CHK_PORT==None:
		try:
			chksvrport = config.get('global', 'chksvrport')
			CHK_PORT = int(chksvrport)
		except:
			pass
	print 'config from file >>> ', 'mdsip:', MDS_IP, 'mdsport:',MDS_PORT, 'vgname:',VGNAME, 'chksvrip:', CHK_IP, 'chksvrport:', CHK_PORT

def default_config():
	global MDS_IP, MDS_PORT, CHK_IP, CHK_PORT, VGNAME
	if MDS_IP==None:
		MDS_IP='192.168.0.149'
	if MDS_PORT==None:
		MDS_PORT=2340
	if CHK_IP==None:
		CHK_IP='192.168.0.149'
	if CHK_PORT==None:
		CHK_PORT=6780
	if VGNAME==None:
		VGNAME='VolGroup'
	
	print 'config from default >>> ', 'mdsip:', MDS_IP, 'mdsport:',MDS_PORT, 'vgname:',VGNAME, 'chksvrip:', CHK_IP, 'chksvrport:', CHK_PORT

def softsan_config():
	config_from_cmd()
	config_from_file()
	default_config()

if __name__=='__main__':
	
	softsan_config()	
	server=ChunkServer()
	logging.basicConfig(level=logging.DEBUG)	
	gevent.spawn(heartBeat, server)
	service=rpc.RpcService(server)
	framework=gevent.server.StreamServer(('0.0.0.0',CHK_PORT), service.handler)
	framework.serve_forever()
	