from pylvm.lvm import *
import logging


class LVM2(LVM):

	def __init__(self):
		LVM.__init__(self)

	#***********************   vg operator  *************************
	def vg_create_active(self, vgname, pvset):
		argv = list()
		argv.append(VGCREATE_BIN_PATH)
		argv.append(vgname)
		argv.append(pvset)
		(status, output) = process_call_argv(argv)
		if status != 0:
			logging.debug("error creating VG")
			return output
		argv.append(VGCHANGE_BIN_PATH)
		argv.append('-a y')
		argv.append(vgname)
		(status, output) = process_call_argv(argv)
		logging.info(output)
		if status != 0:
			return "error activating VG"
		return None

	def vg_remove(self, vgname):
		argv = list()
		argv.append(VGREMOVE_BIN_PATH)
		argv.append('-f')
		argv.append(vgname)
		(status, output) = process_call_argv(argv)
		logging.info(output)
		if status != 0:
			return "error removing VG"
		return None
		

	def vg_reduce(self, vgname, pvname=None):
		argv = list()
		argv.append(VGREDUCE_BIN_PATH)
		if pvname==None:
			argv.append('-a')
			argv.append(vgname)
		else:
			argv.append(vgname)
			argv.append(pvname)
		(status, output) = process_call_argv(argv)
		logging.info(output)
		if status != 0:
			return "error reducing VG"
		return None

	def vg_extend(self, vgname, pvname):
		argv = list()
		argv.append(VGEXTEND_BIN_PATH)
		argv.append(vgname)
		argv.append(pvname)
		(status, output) = process_call_argv(argv)
		logging.info(output)
		if status != 0:
			return "error extending VG"
		return None

	#***********************   pv operator  *************************
	def pv_create(self, pvname):
		argv = list()
		argv.append(PVCREATE_BIN_PATH)
		argv.append('-f')
		argv.append(pvname)
		(status, output) = process_call_argv(argv)
		logging.info(output)
		if status != 0:
			return "error creating PV"
		return None

	def pv_remove(self, pvset):
		argv = list()
		argv.append(PVREMOVE_BIN_PATH)
		argv.append('-f')
		argv.append(pvset)
		(status, output) = process_call_argv(argv)
		logging.info(output)
		if status != 0:
			return "error removing PV"
		return None

	def pv_move(self, pvname):
		argv = list()
		argv.append(PVMOVE_BIN_PATH)
		argv.append(pvname)
		(status, output) = process_call_argv(argv)
		logging.info(output)
		if status != 0:
			return "error moving PV"
		return None

	#***********************   lv operator   *************************
	def lv_create(self, vgname, lvname, size):
		vg = self.get_vgname(vgname)
		unit = size[-1:]
		if unit in LVM.SIZE_UNITS or unit in LVM.EXTENDS_UNITS:
			# lv create command
			argv = list()
			argv.append(LVCREATE_BIN_PATH)
			argv.append("-L" if unit in LVM.SIZE_UNITS else "-l")
			argv.append(size)
			argv.append("-n")		
			argv.append(lvname)
			argv.append(vg.name)
			(status, output) = process_call_argv(argv)
			if status != 0:
				logging.debug('error creating LV')
				return output
			logging.info(output)
			return None
		return "Invalid Size!"
		

	def lv_extend(self, lvpath, alter_size):
		unit = alter_size[-1:]
		if unit in LVM.SIZE_UNITS or unit in LVM.EXTENDS_UNITS:
			argv = list()
			argv.append(LVEXTEND_BIN_PATH)
			argv.append("-f")
			argv.append("-L" if unit in LVM.SIZE_UNITS else "-l")
			argv.append(alter_size)
			argv.append(lvpath)
			(status, output) = process_call_argv(argv)
			logging.info(output)
			if status != 0:
				return "error extending LV"
			return None
		return "Invalid Size!"

	def lv_reduce(self, lvpath, alter_size):
		unit = alter_size[-1:]
		if unit in LVM.SIZE_UNITS or unit in LVM.EXTENDS_UNITS:
			argv = list()
			argv.append(LVREDUCE_BIN_PATH)
			argv.append("-f")
			argv.append("-L" if unit in LVM.SIZE_UNITS else "-l")
			argv.append(alter_size)
			argv.append(lvpath)
			(status, output) = process_call_argv(argv)
			logging.info(output)
			if status != 0:
				return "error reducing LV"
			return None	
		return "Invalid Size!"

	def lv_remove(self, lvpath):
		argv = list()
		argv.append(LVREMOVE_BIN_PATH)
		argv.append("-f")
		argv.append(lvpath)
		(status, output) = process_call_argv(argv)
		logging.info(output)
		if status != 0:
			return "error removING LV"
		return None

	def haslv(self, lvname):
		self.reload()
		for vg in self.vgs:
			for lv in vg.lvs:
				if lv.name==lvname:
					return True
		return False
	


		


	

