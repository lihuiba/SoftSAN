#from pylvm import *
import logging
import os
from ioutilities import process_call_argv
from objects import PhysicalVolume,VolumeGroup,LogicalVolume

LVM_PATH = "/usr/sbin"
LVMDISKSCAN_BIN_PATH = os.path.join(LVM_PATH, 'lvmdiskscan')
if os.access(LVMDISKSCAN_BIN_PATH, os.F_OK) == False: LVM_PATH="/sbin/"
LVM_BIN_PATH = os.path.join(LVM_PATH, 'lvm')
LVMDISKSCAN_BIN_PATH = os.path.join(LVM_PATH, 'lvmdiskscan')
LVDISPLAY_BIN_PATH   = os.path.join(LVM_PATH, 'lvdisplay')
LVCREATE_BIN_PATH    = os.path.join(LVM_PATH, 'lvcreate')
LVCHANGE_BIN_PATH    = os.path.join(LVM_PATH, 'lvchange')
LVCONVERT_BIN_PATH   = os.path.join(LVM_PATH, 'lvconvert')
LVRENAME_BIN_PATH    = os.path.join(LVM_PATH, 'lvrename')
LVEXTEND_BIN_PATH    = os.path.join(LVM_PATH, 'lvextend')
LVREDUCE_BIN_PATH 	= os.path.join(LVM_PATH, 'lvreduce')
LVREMOVE_BIN_PATH 	= os.path.join(LVM_PATH, 'lvremove')
PVCREATE_BIN_PATH 	= os.path.join(LVM_PATH, 'pvcreate')
PVREMOVE_BIN_PATH 	= os.path.join(LVM_PATH, 'pvremove')
PVMOVE_BIN_PATH   	= os.path.join(LVM_PATH, 'pvmove')
VGCREATE_BIN_PATH 	= os.path.join(LVM_PATH, 'vgcreate')
VGCHANGE_BIN_PATH 	= os.path.join(LVM_PATH, 'vgchange')
VGEXTEND_BIN_PATH 	= os.path.join(LVM_PATH, 'vgextend')
VGREDUCE_BIN_PATH 	= os.path.join(LVM_PATH, 'vgreduce')
VGREMOVE_BIN_PATH 	= os.path.join(LVM_PATH, 'vgremove')


class LVM:
	
	EXTENDS_UNITS = "%1234567890"
	SIZE_UNITS    = "GMTPE"

	def __init__(self):
		pass
	
	def load(self):
		self.reload()
		
	def reload(self):
		self.__reload_vgs(); # first query VolumeGroups
		self.__reload_pvs()  # then query PhysicalVolumes
		self.__reload_lvs()  # then query LogicalVolumes

	def create_lv(self, vg, lvname, size):
		unit = size[-1:]
		if unit in LVM.SIZE_UNITS or unit in LVM.EXTENDS_UNITS:
			# lv create command
			argv = list()
			argv.append(LVCREATE_BIN_PATH)
			#argv.append("-t") #uncomment to give an error
			argv.append("-L" if unit in LVM.SIZE_UNITS else "-l")
			argv.append(size)
			argv.append("-n")		
			argv.append(lvname)
			argv.append(vg.name)
			# execute command
			(status, output) = process_call_argv(argv)
			if status != 0:
				logging.debug(output)
				return output
			logging.info(output)
			return None
		return "Invalid Size!"
	
	def remove_lv(self, lv):
		argv = list()
		argv.append(LVREMOVE_BIN_PATH)
		argv.append("-f")
		argv.append(lv.path)
		(status, output) = process_call_argv(argv)
		if status != 0:
			logging.debug("error removing LV")
			return output
		logging.info(output)
		return None
	
	def get_vgname(self, name):
		return self.__vgs[name]
									
	def __reload_vgs(self):
		self.vgs = list()
		self.__vgs = {}
		argv = list()
		argv.append(LVM_BIN_PATH)
		argv.append("vgs")
		argv.append("--nosuffix")
		argv.append("--noheadings")
		argv.append("--units")
		argv.append("b")
		argv.append("--separator")
		argv.append(",")
		argv.append("-o")
		argv.append("vg_name,vg_attr,vg_size,vg_extent_size,vg_free_count,max_lv,max_pv")
		(status, output) = process_call_argv(argv)
		if status != 0:
			logging.debug("error getting list of VG")
			return
		lines = output.splitlines()
		for line in lines:
			line = line.strip()
			words = line.split(",")
			vgname = words[0].strip()
			extent_size = int(words[3])
			extents_total = int(words[2]) / extent_size
			extents_free = int(words[4])
			max_lvs = int(words[5])
			max_pvs = int(words[6])
			if max_lvs == 0: max_lvs = 256
			if max_pvs == 0: max_pvs = 256
			vg = VolumeGroup(vgname, words[1], extent_size, extents_total, extents_free, max_pvs, max_lvs)
			self.__vgs[vgname] = vg
			self.vgs.append(vg);
			
	def __reload_pvs(self):
		self.pvs = list()
		argv = list()
		argv.append(LVM_BIN_PATH)
		argv.append("pvs")
		argv.append("--nosuffix")
		argv.append("--noheadings")
		argv.append("--units")
		argv.append("g")
		argv.append("--separator")
		argv.append(",")
		argv.append("-o")
		argv.append("+pv_pe_count,pv_pe_alloc_count")
		# execute command
		(status, output) = process_call_argv(argv)
		if status != 0:
			logging.debug("error getting list of PV")
			return
		# parse command output
		lines = output.splitlines()
		for line in lines:
			line = line.strip()
			words = line.split(",")
			vgname = words[1]
			if vgname == '': vgname = None				
			pv = PhysicalVolume(words[0], vgname, words[2], words[3], words[4], words[5], True, words[6], words[7])
			self.pvs.append(pv)
			if vgname:
				self.__vgs[vgname].append_pv(pv)

	def __reload_lvs(self):
		self.lvs = list()
		self.lvs_paths = {}
		argv = list()
		argv.append(LVDISPLAY_BIN_PATH)
		argv.append("-c")
		(status, output) = process_call_argv(argv)
		if status != 0:
			logging.debug("error getting list of LV paths")
			return
		# parse command output
		lines = output.splitlines()
		for line in lines:
			line = line.strip()
			words = line.strip().split(":")
			vgname = words[1].strip()
			lvpath = words[0].strip()
			last_slash_index = lvpath.rfind("/") + 1
			lvname = lvpath[last_slash_index:]
			self.lvs_paths[vgname + '`' + lvname] = lvpath
		# lv query command
		argv = list()
		argv.append(LVM_BIN_PATH)
		argv.append("lvs")
		argv.append("--nosuffix")
		argv.append("--noheadings")
		argv.append("--units")
		argv.append("b")
		argv.append("--separator")
		argv.append(";")
		argv.append("-o")
		argv.append("lv_name,vg_name,stripes,stripesize,lv_attr,lv_uuid,devices,origin,snap_percent,seg_start,seg_size,vg_extent_size,lv_size")
		# execute command
		(status, output) = process_call_argv(argv)
		if status != 0:
			logging.debug("error getting list of LV")
			return
		# parse command output
		lines = output.splitlines()
		for line in lines:
			line = line.strip()
			words = line.split(";")
			lvname = words[0].strip()
			vgname = words[1].strip()
			attrs = words[4].strip()
			uuid = words[5].strip()
			extent_size = int(words[11])
			lv_size = int(words[12]) / extent_size
			if vgname == '': vgname = None
			lv = LogicalVolume(lvname, self.lvs_paths[vgname + '`' + lvname], vgname, True, attrs, uuid, lv_size, extent_size)
			if vgname:
				self.__vgs[vgname].append_lv(lv)
				
	def print_out(self):
		for vg in self.vgs:
			print "VG %s (total: %s, allocated: %s, free: %s)" % (vg.name, vg.total, vg.allocated, vg.free)
			print ""
			
			print "\tPV(s): "
			for pv in vg.pvs:
				print "\t%s (total: %s, allocated: %s, free: %s, format: %s)" % (pv.name, pv.total, pv.allocated, pv.free, pv.format)
			print ""
				
			print "\tLV(s): "
			for lv in vg.lvs:
				print "\t%s (path: %s, size: %s)" % (lv.name, lv.path, lv.total)

	

