import rpc, logging
import messages_pb2 as msg
import guid as Guid
import mds_mock
import gevent.server
from lvm_module.my_lvm import *
from tgt_module.tgt_ctrl import *

CHK_PORT=6789
CHK_IP='192.168.0.149'
MDS_IP='192.168.0.149'
MDS_PORT=2345
VGNAME='VolGroup'
LVNAME='lv_softsan_'


def heartBeat():
	global guid
	info=msg.ChunkServerInfo()
	info.ServiceAddress=CHK_IP
	info.ServicePort=CHK_PORT
	info.chunks.add()
	c=info.chunks[0]
	c.guid.a=0x66
	c.guid.b=0x77
	c.guid.c=0x88
	c.guid.d=0x99
	c.size=64
	socket=gevent.socket.socket()
	socket.connect((MDS_IP, MDS_PORT))
	stub=rpc.RpcStub(guid, socket, mds_mock.MDS)
	while True:
		print 'calling ChunkServerInfo'
		stub.callMethod('ChunkServerInfo', info)
		gevent.sleep(3)

class ChunkServer:

	def __init__(self):
		self.chk_dic = {}

		self.tgt = Tgt()
		self.lvm = my_LVM()
		self.tgt.reload()
		self.lvm.reload()
	
# chkserv always uses lun_index=1. if the requested volume does notchkserv 
	def AssembleVolume(self, req):
		ret = msg.AssembleVolume_Response()
		a_guid = req.volume.guid
		if self.chk_dic.get(a_guid)==None:
			ret.error = ('incorrect guid:'+Guid.tostr(a_guid))
			return ret
		target_id = str(a_guid.a)
		target_name = "iqn:softsan_"+Guid.toStr(a_guid)
		acl = 'ALL'
		lun_index = '1'
		lv_name = LVNAME+Guid.toStr(a_guid)
		lun_path = '/dev/'+VGNAME+'/'+lv_name
		self.tgt.new_target(target_id, target_name)
		self.tgt.bind_target(target_id, acl)
		self.tgt.new_lun(target_id, lun_index, lun_path)
		self.tgt.reload()			
		ret.access_point = target_name
		return ret

# why DisassembleVolume_Response return the value of access_point? keep consistent with request
	def DisassembleVolume(self, req):
		ret = msg.DisassembleVolume_Response()
		ret.access_point = req.access_point
		self.tgt.reload()
		target_name = req.access_point
		target_id = self.tgt.path2target_id(target_name)
		if target_id==None;
			ret.error='No such access_point'
		self.tgt.delete_target(target_id)
		return ret

# try to create every requested chunk.however, if some chunk can not be created, fill the ret.error with the output of lvcreate 
	def NewChunk(self, req):
		ret = msg.NewChunk_Response()
		size = str(req.size)+'M'
		for i in range(req.count):
			a_guid = Guid.generate()
			lv_name = LVNAME+Guid.toStr(a_guid)
			lv_size = size
			output =  self.lvm.my_create_lv(VGNAME, lv_name, lv_size)
			if output==None:
				self.lvm.print_out()
				ret.guids.add()
				Guid.assign(ret.guids[-1], a_guid)
				self.chk_dic[a_guid]='logical volume'
			else: 
				ret.error = str(i) + ':' + output + ' '
				continue
		self.lvm.load()
		return ret

# try to delete every requested chunk. if it can not delete, fill the ret.error with output of lvremove
	def DeleteChunk(self, req):
		ret = msg.DeleteChunk_Response()
		for a_guid in req.guids:
			lv_name = LVNAME+Guid.toStr(a_guid)
			lv_path = '/dev/'+VGNAME+'/'+lv_name
			output = self.lvm.my_remove_lv(lv_path)
			if output==None:
				del self.chk_dic[a_guid]
			else:
				error += Guid.toStr(a_guid)+':'+output+' '
				ret.error += error
		self.lvm.load()
		return ret

if __name__=='__main__':
	# guid=msg.Guid()
	# guid.a=9; guid.b=8; guid.c=7; guid.d=6;
	# logging.basicConfig(level=logging.DEBUG)
	# gevent.spawn(heartBeat)
	server=ChunkServer()
	# service=rpc.RpcService(server)
	# framework=gevent.server.StreamServer(('0.0.0.0', CHK_PORT), service.handler)
	# framework.serve_forever()
	
	# mock the newchunk request from client
	req_newchunk=msg.NewChunk_Request()
	req_newchunk.size=32
	req_newchunk.count=1

		

	# test newchunk deletechunk asseble disassemble
	print '     test begin     '.center(100,'-')
	print
	
	# ret_newchunk = server.NewChunk(req_newchunk)

	# mock the delchunk request from client
	# for a_guid in ret_newchunk.guids:
	# 	req_delchunk = msg.DeleteChunk_Request()
	# 	req_delchunk.guids.add()
	# 	Guid.assign(req_delchunk.guids[-1], a_guid)
	
	# ret_delchunk = server.DeleteChunk(req_delchunk)

	# mock the assemblevolume request from client
	# req_assemblevolume=msg.AssembleVolume_Request()
	# Guid.assign(req_assemblevolume.volume.guid, ret_newchunk.guids[-1])
	# req_assemblevolume.volume.size=0
	# req_assemblevolume.volume.parameters=0
	# req_assemblevolume.volume.subvolumes=0

	# ret_assemblevolume = server.AssembleVolume(req_assemblevolume)

	# mock req_disassemblevolume
	# req_disassemblevolume = msg.DisassembleVolume_Response()
	# req_disassemblevolume.access_point = '/dev/VolGroup/lv_softsan_7db4807e-0146b993-55b619a8-a5e88ce3'
	# ret_disassemblevolume = server.DisassembleVolume(req_disassemblevolume)
	# print ret_disassemblevolume.access_point




	print
	print '     test end     '.center(100,'-')
	
