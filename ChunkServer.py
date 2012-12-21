import rpc, logging
import messages_pb2 as msg
import guid as Guid
import mds
import gevent.server
import Backend
from pytgt.tgt_ctrl import *
import util 
import random

MDS_IP='192.168.0.149'
MDS_PORT=2345
CHK_IP='192.168.0.149'
CHK_PORT=6789
VGNAME='VolGroup'
LVNAME='lv_softsan_'

class ChunkServer:

	def __init__(self):
		self.lvm = Backend.LVM_SOFTSAN()
		self.tgt = Backend.TGT_SOFTSAN()
		
	
#  always use lun_index=1. 
	def AssembleVolume(self, req):
		self.tgt.reload()
		ret = msg.AssembleVolume_Response()
		a_guid = req.volume.guid
		lv_name = LVNAME+Guid.toStr(a_guid)
		if not self.lvm.does_lv_exist(lv_name):
			ret.error = (lv_name+'does not exist')
		else:
			target_name = "iqn:softsan_"+Guid.toStr(a_guid)
			target_id = self.tgt.target_name2target_id(target_name)
			if target_id != None:
				ret.access_point = target_name
			else:
				while True:
					target_id = str(random.randint(0,1024*1024))
					if not self.tgt.is_in_targetlist(target_id): 
						break			
				lun_path = '/dev/'+VGNAME+'/'+lv_name
				if self.tgt.assemble(target_id, target_name, lun_path, 'ALL')!=None:
					ret.error = 'Failed to assemble volume'
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
		else:
			if self.tgt.disassemble(target_id)!=None:
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
			if output == None:
				t=ret.guids.add()
				Guid.assign(t, a_guid)
				key=Guid.toTuple(a_guid)
			else: 
				ret.error = str(i) + ':' + output + ' '
		return ret

# try to delete every requested chunk. if it can not delete, fill the ret.error with output of lvremove
	def DeleteChunk(self, req):
		self.lvm.reload()
		ret = msg.DeleteChunk_Response()
		for a_guid in req.guids:
			lv_name = LVNAME+Guid.toStr(a_guid)
			lv_path = '/dev/'+VGNAME+'/'+lv_name
			output = self.lvm.lv_remove(lv_path)
			if output!=None:
				error += Guid.toStr(a_guid) + ':'+ output+ ' '
				ret.error += error
		return ret

#fixme: reconnect to MDS
def heartBeat(server):
	guid=Guid.generate()
	socket=gevent.socket.socket()
	socket.connect((MDS_IP, MDS_PORT))
	stub=rpc.RpcStub(guid, socket, mds.MDS)
	(chunkserver_ip, heartbeat_port)=socket.getsockname()
	while True:
		info=msg.ChunkServerInfo()
		info.ServiceAddress=chunkserver_ip
		info.ServicePort=CHK_PORT
		server.lvm.reload_softsan_lvs()
		for lv in server.lvm.softsan_lvs:
			chk=info.chunks.add()
			name4guid = lv.name.split('lv_softsan_')[1]
			Guid.assign(chk.guid, Guid.fromStr(name4guid))
			chk.size = int(lv.get_sizes(lv.total_extents)[2])
		stub.callMethod('ChunkServerInfo', info)
		gevent.sleep(2)

def test_ChunkServer():

	# print '     test begin     '.center(100,'-')
	# print
	# # mock the newchunk request from client
	# req_newchunk=msg.NewChunk_Request()
	# req_newchunk.size=32
	# req_newchunk.count=1
	# ret_newchunk = server.NewChunk(req_newchunk)

	# # mock the assemblevolume request from client
	# req_assemblevolume=msg.AssembleVolume_Request()
	# Guid.assign(req_assemblevolume.volume.guid, ret_newchunk.guids[-1])
	# req_assemblevolume.volume.size=32
	# ret_assemblevolume = server.AssembleVolume(req_assemblevolume)

	# # # mock req_disassemblevolume
	# # req_disassemblevolume = msg.DisassembleVolume_Response()
	# # req_disassemblevolume.access_point = ret_assemblevolume.access_point
	# # ret_disassemblevolume = server.DisassembleVolume(req_disassemblevolume)
	# # print ret_disassemblevolume.access_point

	# # # mock the delchunk request from client
	# # req_delchunk = msg.DeleteChunk_Request()
 	# # for a_guid in ret_newchunk.guids:
	# # 	t=req_delchunk.guids.add()
	# # 	Guid.assign(t, a_guid)
	# # ret_delchunk = server.DeleteChunk(req_delchunk)


	# print
	# print '     test end     '.center(100,'-')

if __name__=='__main__':
	
	server=ChunkServer()
	logging.basicConfig(level=logging.DEBUG)	
	gevent.spawn(heartBeat, server)
	service=rpc.RpcService(server)
	framework=gevent.server.StreamServer(('0.0.0.0',CHK_PORT), service.handler)
	framework.serve_forever()
	
