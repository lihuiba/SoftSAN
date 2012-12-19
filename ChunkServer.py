import rpc, logging
import messages_pb2 as msg
import guid as Guid
import mds
import gevent.server
from pylvm2.lvm_ctrl import *
from pytgt.tgt_ctrl import *
import util 

MDS_IP='192.168.0.149'
MDS_PORT=2345
CHK_IP='192.168.0.149'
CHK_PORT=6789
VGNAME='VolGroup'
LVNAME='lv_softsan_'


class ChunkServer:

	def __init__(self):
		self.tgt = Tgt()
		self.lvm = LVM2()
		self.tgt.reload()
		self.lvm.reload()
	
# chkserv always uses lun_index=1. if the requested volume does notchkserv 
	def AssembleVolume(self, req):
		ret = msg.AssembleVolume_Response()
		a_guid = req.volume.guid
		target_id = str(a_guid.a%65536)
		target_name = "iqn:softsan_"+Guid.toStr(a_guid)
		acl = 'ALL'
		lun_index = '1'
		lv_name = LVNAME+Guid.toStr(a_guid)
		lun_path = '/dev/'+VGNAME+'/'+lv_name
		if self.tgt.assemble(target_id, target_name, lun_path, acl)!=None:
			ret.error = 'Failed to assemble volume'
			return ret
		self.tgt.reload()			
		ret.access_point = target_name
		return ret
		
# why DisassembleVolume_Response return the value of access_point? keep consistent with request
	def DisassembleVolume(self, req):
		ret = msg.DisassembleVolume_Response()
		ret.access_point = req.access_point
		self.tgt.reload()
		target_name = req.access_point
		target_id = self.tgt.target_name2target_id(target_name)
		if target_id==None:
			ret.error='No such access_point'
			return ret
		if self.tgt.delete_target(target_id)!=None:
			ret.error='failed to unbind target'
		return ret

# try to create every requested chunk.however, if some chunk can not be created, fill the ret.error with the output of lvcreate 
	def NewChunk(self, req):
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
				continue
		self.lvm.reload()
		return ret

# try to delete every requested chunk. if it can not delete, fill the ret.error with output of lvremove
	def DeleteChunk(self, req):
		ret = msg.DeleteChunk_Response()
		for a_guid in req.guids:
			lv_name = LVNAME+Guid.toStr(a_guid)
			lv_path = '/dev/'+VGNAME+'/'+lv_name
			output = self.lvm.lv_remove(lv_path)
			if output!=None:
				error += Guid.toStr(a_guid)+':'+output+' '
				ret.error += error
		self.lvm.reload()
		return ret

# untested function, it did not fill all the attribute of chunkserverInfo
def heartBeat(server):
	global guid
	socket=gevent.socket.socket()
	socket.connect((MDS_IP, MDS_PORT))
	stub=rpc.RpcStub(guid, socket, mds.MDS)
	(myip, myport)=socket.getsockname()
	print myip, ':', myport
	while True:
		print 'calling ChunkServerInfo'
		info=msg.ChunkServerInfo()
		info.ServiceAddress=CHK_IP
		info.ServicePort=CHK_PORT
		server.lvm.read_lvs()
		for lv in server.lvm.softsan_lvs:
			info.chunks.add()
			chk=info.chunks[-1]
			name4guid = lv.name.split('lv_softsan_')[1]
			Guid.assign(chk.guid, Guid.fromStr(name4guid))
			chk.size = int(filter(lambda ch: ch in '0123456789.', lv.total).split('.')[0])
			print 'name4guid:', name4guid, 'chk.size:',chk.size
		stub.callMethod('ChunkServerInfo', info)
		gevent.sleep(2)


if __name__=='__main__':

	# server=ChunkServer()
	# # logging.basicConfig(level=logging.DEBUG)
	# # gevent.spawn(heartBeat)
	# # service=rpc.RpcService(server)
	# # framework=gevent.server.StreamServer(('0.0.0.0', CHK_PORT), service.handler)
	# # framework.serve_forever()
		
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
 # # 	for a_guid in ret_newchunk.guids:
	# # 	t=req_delchunk.guids.add()
	# # 	Guid.assign(t, a_guid)
	# # ret_delchunk = server.DeleteChunk(req_delchunk)


	# print
	# print '     test end     '.center(100,'-')
	server=ChunkServer()
	guid=msg.Guid()
	guid.a=1; guid.b=8; guid.c=2; guid.d=1
	logging.basicConfig(level=logging.DEBUG)	
	gevent.spawn(heartBeat, server)
	service=rpc.RpcService(server)
	framework=gevent.server.StreamServer(('0.0.0.0',CHK_PORT), service.handler)
	framework.serve_forever()
	
