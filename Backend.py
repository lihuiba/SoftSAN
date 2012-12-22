import logging
import guid as Guid
import pylvm2.lvm_ctrl 
import pytgt.tgt_ctrl
import random
from util import *


class LVM_SOFTSAN(pylvm2.lvm_ctrl.LVM2):
	"""docstring for LVM_SOFTSAN"""
	def __init__(self):
		self.reload_softsan_lvs()

	def reload_softsan_lvs(self, vgname_softsan=None):
		self.reload()
		# self.softsan_lvs=[lv for vg in self.vgs for lv in vg if lv.name.startswith('lv_softsan_')]
		self.softsan_lvs = []
		for vg in self.vgs:
			for lv in vg.lvs:
				if lv.name.find('lv_softsan_')==0:
					self.softsan_lvs.append(lv)
		return None

	# fixme: should be put into pylvm2.lvm_ctrl.LVM2, and change the name to 'haslv(self, lvname)'  	
	def does_lv_exist(self, lvname):
		self.reload()
		for vg in self.vgs:
			for lv in vg.lvs:
				if lv.name==lvname:
					return True
		return False

class TGT_SOFTSAN(pytgt.tgt_ctrl.Tgt):
	"""docstring for TGT_SOFTSAN"""
	def __init__(self):
		pytgt.tgt_ctrl.Tgt.__init__(self)
		self.reload()

	#fixme: it's not suitable to call it 'assemble' here
	def assemble(self, target_id, target_name, lun_path, acl='ALL'):
		output = self.new_target(target_id, target_name) 
		if output != None:
 			# fixme: replace print with logging
			print 'Assemble failure:', output
			return output
		output = self.bind_target(target_id, acl)
		if output != None:
			self.delete_target(target_id)
 			# fixme: replace print with logging
			print 'Assemble failure:', output
			return output
		output = self.new_lun(target_id, lun_path)
		if output != None:
			self.delete_target(target_id)
 			# fixme: replace print with logging
			print 'Assemble failure:', output
			return output
 			# fixme: replace print with logging
		print 'Assemble successfully, target_id:', target_id, 'path:', lun_path
		return None

	def disassemble(self, target_id):
		output = self.delete_target(target_id)
		if output==None:
 			# fixme: replace print with logging
			print ' Disassemble target successfully, target id:', target_id
			return None
		return output
