import rpc, logging
import messages_pb2 as msg
import guid as Guid
import mds, config, util
import gevent.server
import Backend
from pytgt.tgt_ctrl import *
import random


class ChunkServer:
	def __init__(self, prefix_vol='lv_softsan_', vgname='VolGroup'):
		self.lvm = Backend.LVM_SOFTSAN()
		self.tgt = Tgt()
		self.prefix_vol = prefix_vol
		self.vgname = vgname
	
	#  always use lun_index=1. 
	def AssembleVolume(self, req):
		self.tgt.reload()
		ret = msg.AssembleVolume_Response()
		str_guid = Guid.toStr(req.volume.guid)
		lv_name = self.prefix_vol+str_guid
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
		lun_path = '/dev/'+self.vgname+'/'+lv_name
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

	# try to create every requested chunk. however, if some chunk can not be created, fill the ret.error with the output of lvcreate 
	def NewChunk(self, req):
		self.lvm.reload()
		ret = msg.NewChunk_Response()
		size = str(req.size)+'M'
		for i in range(req.count):
			a_guid = Guid.generate()
			lv_name = self.prefix_vol+Guid.toStr(a_guid)
			lv_size = size
			output =  self.lvm.lv_create(self.vgname, lv_name, lv_size)
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
		print 'ChunkServer:  DeleteChunk'
		for a_guid in req.guids:
			str_guid=Guid.toStr(a_guid)
			lv_name = self.prefix_vol+str_guid
			lv_path = '/dev/'+self.vgname+'/'+lv_name
			output = self.lvm.lv_remove(lv_path)
			if output!=None:
				ret.error = "Unable to delete chunk {0}:\n{1}".format(str_guid, output)
				break
			t=ret.guids.add()
			Guid.assign(t, a_guid)
		return ret

	def doHeartBeat(self, serviceport, stub):
		serviceip=stub.socket.getsockname()[0]
		while True:
			info=msg.ChunkServerInfo()
			info.ServiceAddress=serviceip
			info.ServicePort=serviceport
			self.lvm.reload_softsan_lvs()
			for lv in self.lvm.softsan_lvs:
				chk=info.chunks.add()
				name4guid = lv.name.split(self.prefix_vol)[1]
				Guid.assign(chk.guid, Guid.fromStr(name4guid))
				chk.size = int(lv.get_sizes(lv.total_extents)[2])
			stub.callMethod('ChunkServerInfo', info)
			print 'for test--------------------------------', random.randint(0,100)
			gevent.sleep(1)

	def heartBeat(self, confobj):
		guid=Guid.generate()
		stub=rpc.RpcStub(guid, None, mds.MDS)
		while True:
			try:
				socket=gevent.socket.socket()
				socket.connect((confobj.mdsaddress, int(confobj.mdsport)))
				mdsEndpoint=(confobj.mdsaddress, int(confobj.mdsport))
				socket.connect(mdsEndpoint)				
				stub.socket=socket
				self.doHeartBeat(int(confobj.port), stub)
			except KeyboardInterrupt:
				raise
			except:
				logging.debug('An error occured during heart beat, preparing to retry', exc_info=1)
				gevent.sleep(2)

def test_ChunkServer():
	print '     test begin     '.center(100,'-')
	print
	server=ChunkServer()
	logging.basicConfig(level=logging.DEBUG)

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

	# # mock req_disassemblevolume
	req_disassemblevolume = msg.DisassembleVolume_Request()
	req_disassemblevolume.access_point = ret_assemblevolume.access_point
	ret_disassemblevolume = server.DisassembleVolume(req_disassemblevolume)

	# mock the delchunk request from client
	req_delchunk = msg.DeleteChunk_Request()
 	for a_guid in ret_newchunk.guids:
		t=req_delchunk.guids.add()
		Guid.assign(t, a_guid)
	ret_delchunk = server.DeleteChunk(req_delchunk)
	print
	print '     test end     '.center(100,'-')



if __name__=='__main__':
	pass
	# link_test()
	#test_ChunkServer()
