from tgt_module.tgt_ctrl import *
from lvm_module.my_lvm import *
# import lvm_module.my_lvm
# from guid import *
import guid

def lvm_test():
	lvm = my_LVM()
	lvm.load()
	lvm.print_out()
	create = raw_input('create a new lvm?(y/n)')
	vgname = 'vg0'
	lvname = 'lvdefault'
	size = '64M'
		
	if create=='y':
		lvm.my_create_lv(vgname, lvname, size)
	lvm.load()
	lvm.print_out()
	delete = raw_input('delete it?(y/n)')
	
	if delete == 'y':
		lvpath = ('/dev/'+vgname+'/'+lvname).strip()
		lvm.my_remove_lv(lvpath)
	lvm.load()
	lvm.print_out()


def tgt_test():
	tgt = Tgt()
	tgt.reload()
	tgt.print_out()

	while True:
		new = raw_input('you want to new target?(y/n)')
		if new=='y':
			tgt.new_target('11','test:iscsi')
			tgt.bind_target('11','ALL')
			tgt.new_lun('11','1','/dev/vg0/lv0')
		tgt.reload()
		tgt.print_out()
		delete = raw_input('you want to delete target?(y/n)')
		if delete=='y':
			tgt.delete_target('11')
		tgt.reload()
		tgt.print_out()

def combine():
	tgt = Tgt()
	tgt.reload()
	tgt.print_out()
	lvm = my_LVM()
	lvm.load()
	lvm.print_out()
	ret = guid.generate()
	name = guid.toStr(ret)
	lvname = 'lv_' + name
	vgname = 'vg0'
	lvpath = ('/dev/'+vgname+'/'+lvname).strip()
	tgtname = 'iqn:'+name
	size = '64M'

	while True:
		tgt.reload()
		lvm.load()
		lvm.print_out()
		tgt.print_out()
		want = raw_input("input your command:(new_target,new_lun,new_lvm,delete_lvm,delete_target)")
		if want == 'new_target':
			tgt.new_target('11',tgtname)
			tgt.bind_target('11','ALL')
			continue
		elif want == 'new_lun':
			tgt.new_lun('11','1',lvpath)
			continue
		elif want == 'new_lvm':
			lvm.my_create_lv(vgname, lvname, size)
			continue
		elif want == 'delete_target':
			tgt.delete_target('11')
			continue
		elif want == 'delete_lvm':
			lvm.my_remove_lv(lvpath)
			continue
			
		



if __name__ == '__main__':
	combine()