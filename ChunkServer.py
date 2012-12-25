import rpc, logging
import messages_pb2 as msg
import guid as Guid
import mds, config
import gevent.server
import Backend
from pytgt.tgt_ctrl import *
import random

MDS_IP='192.168.0.149'
MDS_PORT=2340
CHK_IP='192.168.0.149'
CHK_PORT=67802

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
		print 'for test--------------------------------', random.randint(0,100)
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


if __name__=='__main__':
	cfgdict = {'MDS_IP':['M','192.168.0.149'], 'MDS_PORT':['m','6789'], \
				'CHK_IP':['C','192.168.0.149'], 'CHK_PORT':['c','3456'],'enablexxx':['x',False]}
	cfgfile = '/home/hanggao/SoftSAN/test.conf'
	configure = config.config(cfgdict, cfgfile)
	MDS_IP = configure['MDS_IP']
	MDS_PORT = int(configure['MDS_PORT'])
	CHK_IP = configure['CHK_IP']
	CHK_PORT = int(configure['CHK_PORT'])

	server=ChunkServer()
	logging.basicConfig(level=logging.DEBUG)	
	gevent.spawn(heartBeat, server)
	service=rpc.RpcService(server)
	framework=gevent.server.StreamServer(('0.0.0.0',CHK_PORT), service.handler)
	framework.serve_forever()
	