from pylvm import  *

class my_LVM(LVM):
	
	# my function: create lv
	def my_create_lv(self, vgname, lvname, size):
		vg = self.get_vgname(vgname)
		self.create_lv(vg, lvname, size)

	# my function: extend the lv
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
				print "error extending LV"
				return output
			print output
			return None
		return "Invalid Size!"

	# my function: reduce the lv
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
				print "error reducing LV"
				return output
			print output
			return None
		return "Invalid Size!"

	# my function: remove lv
	def my_remove_lv(self, lvpath):
		argv = list()
		argv.append(LVREMOVE_BIN_PATH)
		#argv.append("-t")
		argv.append("-f")
		argv.append(lvpath)
		
		# execute command
		(status, output) = process_call_argv(argv)
		if status != 0:
			print "error removing LV"
			return output
		print output
		return None




	

