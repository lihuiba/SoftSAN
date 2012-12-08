import dm

class DeviceMapper:
	def __init__(self):
		pass
			
	#device_name specifies the name of the device, table_file is the configuration file of this device
	def create(self, device_name, table_file):
		args = ['create']
		args.append(device_name)
		args.append(table_file)
		dm.Get_Command(args)

	#device_name:specifies which device is going to be removed
	#device_name: 1 device name 2 <major, minor> 3 UUID
	def remove(self, device_name):
		args = ['remove']
		args.append(device_name)
		dm.Get_Command(args)

	#device_name:specifies which device is going to be removed
	#device_name: now only device name is acceptable
	def info(self, device_name):
		args = ['info']
		args.append(device_name)
		dm.Get_Command(args)