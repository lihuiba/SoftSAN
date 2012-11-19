# 
# Copyright since 2012 liyongchuan, nudt pdl.
# 
# This is a simple python-api to deal with device mapper
# mapping table file is the configuration file which contains formatted tables to construct a mapped device
# some of the functions must be given the location of the table, like create etc..
# examples:
# 1) 0      1024 linear /dev/sda  204
#    1024  512   linear /dev/sdb  766
#    1536  128   linear /dev/sdc 0
# 2)	0	2048	striped	2	64	/dev/sda	1024	/dev/sdb	0
# 3)	0	4711	 mirror	core	 2	64	nosync	2	/dev/sda	2048	/dev/sdb 1024


import Dmsetup_c as _dm

class DmController:
	def __init__(self):
		self.argc = 0
		self.argv = list()
		
		#self.switches = list()
		#self.table = list()

#	def construct(self, command):
#		if command == 'create':
#			argv[0] = command

		
	#def execute(self, argc, argv):
	#	_dm.execute(argc, argv)

	
	#device_name specifies the name of the device, table_file is the configuration file of this device
	def create(self, device_name, table_file):
		argc = 3
		argv.insert(0, "create")
		argv.insert(1, device_name)
		argv.insert(2, table_file)
		
		_dm.execute(argc, argv)

	#device_name specifies the name of the device which is going to be removed
	def remove(self, device_name):
		argc = 2
		argv.insert(0, "remove")
		argv.insert(1, device_name)
		
		_dm.execute(argc, argv)

#remove all mapped device, force to detemine if or not to force stop devices
	def remove_all(self, force = None):
		argv.insert(0, "remove_all")
		if force is None:
			argc = 2
			argv.append(2, "force")
		else
			argc = 1
			
		_dm.execute(argc, argv)

	def suspend(self, device_name):
		argc = 2
		argv.insert(0, "suspend")
		argv.insert(1, device_name)
		
		_dm.execute(argc, argv)
		
	def resume(self, device_name):
		argc = 2
		argv.insert(0, "resume")
		argv.insert(1, device_name)
		
		_dm.execute(argc, argv)

#load a table to create a device
	def load(self, device_name, table_file):
		argc = 3
		argv.insert(0, "load")
		argv.insert(1, device_name)
		argv.insert(2, table_file)
		
		_dm.execute(argc, argv)

	def reload(self, device_name, table_file):
		argc = 3
		argv.insert(0, "reload")
		argv.insert(1, device_name)
		argv.insert(2, table_file)
		
		_dm.execute(argc, argv)
		
	def clear(self, device_name):
		argc = 2
		argv.insert(0, "clear")
		argv.insert(1, device_name)
		
		_dm.execute(argc, argv)
		
	def rename(self, device_name, new_name):
		argc = 3
		argv.insert(0, "rename")
		argv.insert(1, device_name)
		argv.insert(2, new_name)
		
		_dm.execute(argc, argv)
		
	def message(self, device, sector, data):
		argc = 4
		argv.insert(0, "message")
		argv.insert(1, device)
		argv.insert(2, sector)
		argv.insert(3, data)
		
		_dm.execute(argc, argv)
		
	def ls(self):
		argc = 1
		argv.insert(0, "ls")

		_dm.execute(argc, argv)
		
	def info(self, device):
		argc = 2
		argv.insert(0, "info")
		argv.insert(1, device)
		
		_dm.execute(argc, argv)
		
	def deps(self, device = None):
		argv.insert(0, "deps")
		if device is None:
			argc = 1
		else
			argc = 2
			argv.insert(1, device)

		_dm.execute(argc, argv)
		
	def status(self, device):
		argc = 2
		argv.insert(0, "status")
		argv.insert(1, device)
		
		_dm.execute(argc, argv)
		
	def wait(self, device, event_nr = None):
		argv.insert(0, "wait")
		argv.insert(1, device)
		if event_nr is None:
			argc = 2
		else
			argc = 3
			argv.insert(2, event_nr)

		_dm.execute(argc, argv)
		
	def mknodes(self, device = None):
		argv.insert(0, "mknodes")
		if device is None:
			argc = 1
		else
			argc = 2
			argv.insert(1, device)

		_dm.execute(argc, argv)
		
	def targets(self):
		argc = 1
		argv.insert(0, "targets")
		
		_dm.execute(argc, argv)
		
	def version(self):
		argc = 1
		argv.insert(0, "version")
		
		_dm.execute(argc, argv)
		
	def setgeometry(self, device, cyl, head, sect, start):
		argc = 5
		argv.insert(0, "setgeometry")
		argv.insert(1, device)
		argv.insert(2, cyl)
		argv.insert(3, head)
		argv.insert(2, sect)
		argv.insert(3, start)
		
		_dm.execute(argc, argv)
