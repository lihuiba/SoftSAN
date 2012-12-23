import pylvm2.lvm_ctrl 


class LVM_SOFTSAN(pylvm2.lvm_ctrl.LVM2):
	"""docstring for LVM_SOFTSAN"""
	def __init__(self):
		pylvm2.lvm_ctrl.LVM2.__init__(self)
		self.reload_softsan_lvs()

	def reload_softsan_lvs(self):
		self.reload()
		self.softsan_lvs=[lv for vg in self.vgs for lv in vg.lvs if lv.name.startswith('lv_softsan_')]
		return None





	
