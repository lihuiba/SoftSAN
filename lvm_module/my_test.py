#!/usr/bin/env python
from my_lvm import *


def help():
	print 'your command should be as follow:'
	print
	print 'lvcreate vg_name(vgx) lv_name(lvx) size(xxxG/M)'
	print 'lvremove lv_path(/dev/vgx/lvx)'
	print 'lvextend lv_path(/dev/vgx/lvx) [+/-]size(+10G, -128M)'
	print 'lvreduce lv_path(/dev/vgx/lvx) [+/-]size(+10G, -128M)'
	print
	print 'pvcreate pv_name(/dev/sda1)'
	print 'pvremove pv_name Use only after the PV had been removed from a VG with vgreduce'
	print 'pvmove pv_name'
	print
	print 'vgcreate vg_name pv0_name pv1_name ...'
	print 'vgextend vg_name pv_name'
	print 'vgreduce vg_name '
	print 'vgremove vg_name '



def test_lvm():
	lvm = my_LVM()
	lvm.load()
	lvm.print_out()
	print '<<<<<<<<<<<<   Welcome to lvm controllor  >>>>>>>>>>>>>'
	help()
	while True:
		inputs = raw_input('command >>> ')
		cmd = inputs.split()
		l_cmd = len(cmd)

		if l_cmd==4 and cmd[0]=='lvcreate':
			lvm.my_create_lv(cmd[1], cmd[2], cmd[3])
			lvm.load()
		elif l_cmd==3 and cmd[0]=='lvextend':
			lvm.my_extend_lv(cmd[1], cmd[2])
			lvm.load()
		elif l_cmd==3 and cmd[0]=='lvreduce':
			lvm.my_reduce_lv(cmd[1], cmd[2])
			lvm.load()
		elif l_cmd==2 and cmd[0]=='lvremove':
			lvm.my_remove_lv(cmd[1])
			lvm.load()
		elif cmd[0]=='vgcreate':
			lvm.my_create_active_vg(cmd[1], cmd[2])
			lvm.load()
		elif cmd[0]=='vgremove':
			lvm.my_remove_vg(cmd[1])
			lvm.load()
		elif cmd[0]=='vgreduce':
			lvm.my_reduce_vg(cmd[1], cmd[2])
			lvm.load()
		elif cmd[0]=='vgextend':
			lvm.my_extend_vg(cmd[1], cmd[2])
			lvm.load()
		elif cmd[0]=='pvcreate':
			lvm.my_create_pv(cmd[1])
			lvm.load()
		elif cmd[0]=='pvremove':
			lvm.my_remove_pv(cmd[1])
			lvm.load()
		elif cmd[0]=='pvmove':
			lvm.my_move_pv(cmd[1])
			lvm.load()

		elif cmd[0]=='quit' or cmd[0]=='q':
			break
		elif cmd[0]=='print' or cmd[0]=='p':
			lvm.load()
			lvm.print_out()
		elif cmd[0]=='help' or cmd[0]=='h':
			help()
		else:
			print
			print '---------   input error!   ----------'
			print 
			help()
		print 

if __name__ == '__main__':
	test_lvm()
