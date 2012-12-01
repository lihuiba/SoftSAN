from ctypes import *

dm = cdll.LoadLibrary('/home/yongchuan/workspace/dmdebug/dmtest.so')

class DeviceMapper:
	def __init__(self):
		self.argc = 0
		self.argv = ''
			
	#device_name specifies the name of the device, table_file is the configuration file of this device
	def create(self, device_name, table_file):
		argc = 4
		argv = 'dm create' + ' ' + device_name + ' ' + table_file
		dm.DmAPI(argc, argv)

	#device_name:specifies which device is going to be removed
	#device_name: 1 device name 2 <major, minor> 3 UUID
	def remove(self, device_name):
		argc = 3
		argv = 'dm remove' + ' ' + device_name
		dm.DmAPI(argc, argv)
