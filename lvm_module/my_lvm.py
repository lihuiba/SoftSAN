from lvm import *


class my_LVM(LVM):


	#***********************   vg operator  *************************

	def my_create_active_vg(self, vgname, pvset):
		argv = list()
		argv.append(VGCREATE_BIN_PATH)
		argv.append(vgname)
		argv.append(pvset)
		(status, output) = process_call_argv(argv)
		if status != 0:
			print "error create VG"
			return output
		argv.append(VGCHANGE_BIN_PATH)
		argv.append('-a y')
		argv.append(vgname)
		print argv
		(status, output) = process_call_argv(argv)
		if status != 0:
			print "error activate VG"
			return output
		print output
		return None

	def my_remove_vg(self, vgname):
		argv = list()
		argv.append(VGREMOVE_BIN_PATH)
		argv.append('-f')
		argv.append(vgname)
		(status, output) = process_call_argv(argv)
		if status != 0:
			print "error remove VG"
			return output
		print output
		return None

	def my_reduce_vg(self, vgname, pvname=None):
		argv = list()
		argv.append(VGREDUCE_BIN_PATH)
		if pvname==None:
			argv.append('-a')
			argv.append(vgname)
		else:
			argv.append(vgname)
			argv.append(pvname)
		(status, output) = process_call_argv(argv)
		if status != 0:
			print "error reduce VG"
			return output
		print output
		return None

	def my_extend_vg(self, vgname, pvname):
		argv = list()
		argv.append(VGEXTEND_BIN_PATH)
		argv.append(vgname)
		argv.append(pvname)
		(status, output) = process_call_argv(argv)
		if status != 0:
			print "error extend VG"
			return output
		print output
		return None

	#***********************   pv operator  *************************

	def my_create_pv(self, pvname):
		argv = list()
		argv.append(PVCREATE_BIN_PATH)
		argv.append('-f')
		argv.append(pvname)
		(status, output) = process_call_argv(argv)
		if status != 0:
			print "error create pv"
			return output
		print output
		return None

	def my_remove_pv(self, pvset):
		argv = list()
		argv.append(PVREMOVE_BIN_PATH)
		argv.append('-f')
		argv.append(pvset)
		(status, output) = process_call_argv(argv)
		if status != 0:
			print "error remove pv"
			return output
		print output
		return None

	def my_move_pv(self, pvname):
		argv = list()
		argv.append(PVMOVE_BIN_PATH)
		argv.append(pvname)
		(status, output) = process_call_argv(argv)
		if status != 0:
			print "error move pv"
			return output
		print output
		return None

	#***********************   lv operator   *************************

	def my_create_lv(self, vgname, lvname, size):
		vg = self.get_vgname(vgname)
		self.create_lv(vg, lvname, size)

	def my_extend_lv(self, lvpath, alter_size):
		unit = alter_size[-1:]
		if unit in LVM.SIZE_UNITS or unit in LVM.EXTENDS_UNITS:
			argv = list()
			argv.append(LVEXTEND_BIN_PATH)
			argv.append("-f")
			argv.append("-L" if unit in LVM.SIZE_UNITS else "-l")
			argv.append(alter_size)
			argv.append(lvpath)
			
			(status, output) = process_call_argv(argv)
			if status != 0:
				print "error extend lv"
				return output
			print output
			return None
		return "Invalid Size!"

	def my_reduce_lv(self, lvpath, alter_size):
		unit = alter_size[-1:]
		if unit in LVM.SIZE_UNITS or unit in LVM.EXTENDS_UNITS:
			argv = list()
			argv.append(LVREDUCE_BIN_PATH)
			argv.append("-f")
			argv.append("-L" if unit in LVM.SIZE_UNITS else "-l")
			argv.append(alter_size)
			argv.append(lvpath)
			
			(status, output) = process_call_argv(argv)
			if status != 0:
				print "error reduce lv"
				return output
			print output
			return None
		return "Invalid Size!"

	def my_remove_lv(self, lvpath):
		argv = list()
		argv.append(LVREMOVE_BIN_PATH)
		#argv.append("-t")
		argv.append("-f")
		argv.append(lvpath)
		
		# execute command
		(status, output) = process_call_argv(argv)
		if status != 0:
			print "error remove lv"
			return output
		print output
		return None




	

