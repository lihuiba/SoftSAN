import Client
from argparse import ArgumentParser as ArgParser
from mds import Object

CHUNKSIZE = 64

def ParseLine(words):
	arg = Object()
	arg.name = ''
	arg.size = 0
	arg.type = ''
	arg.chunksizes = []
	arg.subvolumes = []
	arg.parameters = []
	tableFormat = 'name size [type] [num] [sizes|volumenames]'
	num = len(words)
	argc = 0
	if num < 2:
		print 'Too few arguments'
		return None
	if isinstance(words[0], str)==False or words[1].isdigit()==False:
		print tableFormat
		return None
	arg.name = words[0]
	arg.size = int(words[1])
	argc += 2
	if argc >= num:
		return arg

	if words[argc].isdigit() == False:
		if words[argc] == 'striped':
			arg.type = 'striped'
		elif words[argc] == 'linear':				
			arg.type = 'linear'
		elif words[argc] == 'gfs':
			arg.type = 'gfs'
			return arg
		else:
			error = 'Unrecognized argument: ' + words[argc]
			print error
		argc += 1
		if argc >= num:
			return arg
	if words[argc].isdigit():
		num_vol = int(words[argc])
		argc += 1
		if argc >= num:
			chksize = arg.size/num_vol
			if chksize < 4:
				print 'chunk size too samll'
				return None
			totsize = arg.size
			while totsize > chksize:
				arg.chunksizes.append(chksize)
				totsize -= chksize
			if totsize > 0:
				arg.chunksizes.append(totsize)
			return arg
		if words[argc].isdigit():
			totsize = 0
			for i in range(argc, num):
				arg.chunksizes.append(int(words[i]))
				totsize += int(words[i])
			if totsize > arg.size:
				print 'total chunk size summary is larger than the size you specifed'
				return None
			remain = arg.size-totsize
			while remain > CHUNKSIZE:
				arg.chunksizes.append(CHUNKSIZE)
				remain -= CHUNKSIZE
			if remain > 0:
				arg.chunksizes.append(remain)
			return arg
		else:
			if num-argc != num_vol:
				return None
			for i in range(argc, num):
				arg.subvolumes.append(words[i])
			return arg

def CheckName(mds, name):
	if name == '':
		return ''
	if not isinstance(name, str):
		return  'Name should be a string!'
	if mds.ReadVolumeInfo(name) == None:
		return 'Volume ' + name + ' is not exsit!'
	return ''

def ParseArgs(client):
	parser = ArgParser()
	subparsers = parser.add_subparsers()
	# create
	parser_create = subparsers.add_parser('create', help='create a volume')
	parser_create.add_argument('table', nargs='+', help='table to build a volume')
	parser_create.add_argument('--file', dest='path', help='table file')
	parser_create.add_argument('--params', '-p', help='voluem parameters')
	parser_create.set_defaults(func='Create')
	# remove
	parser_remove = subparsers.add_parser('remove', help='remove a volume')
	parser_remove.add_argument('volume_name', help='volume to be removed')
	parser_remove.set_defaults(func='Delete')
	# list
	parser_list = subparsers.add_parser('list', help='show volume info')
	parser_list.add_argument('volume_name', help='volume to be removed')
	parser_list.set_defaults(func='List')
	# split
	parser_split = subparsers.add_parser('split', help='split a volume')
	parser_split.add_argument('volume_name', help='volume to be removed')
	parser_split.set_defaults(func='Split')
	# mount
	parser_mount = subparsers.add_parser('mount', help='mount a volume')
	parser_mount.add_argument('volume_name', help='volume to be removed')
	parser_mount.set_defaults(func='Mount')
	# unmount
	parser_unmount = subparsers.add_parser('unmount', help='unmount a volume')
	parser_unmount.add_argument('volume_name', help='volume to be removed')
	parser_unmount.set_defaults(func='Unmount')

	#args = parser.parse_args('create striped_device 180 striped 2 90 90'.split())
	#args = parser.parse_args('create striped_device_2 18 striped 3 6 6 6'.split())
	#args = parser.parse_args('remove striped_device_2'.split())
	#args = parser.parse_args('remove hello_softsan_striped_2'.split())
	print args

	if args.func == 'Create':
		data = ParseLine(args.table)
		if isinstance(data, Object) == False:
			print 'table format error'
			return None
		if args.params:
			strsize = int(args.params)
			# if strsize != 256 and strsize != 512 and strsize != 1024 and strsize != 2048:
			# 	print 'stripe size only can be: 256/512/1024/2048'
			# 	return None
			data.parameters.append(args.params)
	else:
		data = args.volume_name
		error = CheckName(client.mds, data)
		if not error == '':
			print error
			return None
	
	getattr(client, args.func+'Volume')(data)
	return data


# def ParseFile(path):
# 	fp = open(path)
# 	lines = fp.readlines()
# 	for line in lines:
# 		ParseLine(line)

# def ParseArg(client):
# 	ArgsDict={'create' :['c',  '',       'create a volume'],
# 			  'table'  :['t',  sys.stdin,'volume construction table'],
# 			  'remove' :['rm', '',		 'remove a volume'],
# 			  'list'   :['l',  '',		 'list object information'],
# 			  'unmap'  :['',   '',       'split a volume into subvolumes'],
# 			  'mount'  :['',   '',       'Mount a exist volume'],
# 			  'unmount':['',   '',		 'Unmount a volume']
# 	}
# 	ArgsFile = 'test.conf'
# 	args = Object(config.config(ArgsDict, ArgsFile))

# 	print args.create

	# if hasattr(args, 'create'):
	# 	print args.create
	# 	#arg = ParseLine(args.create)
	# 	#print arg.name, arg.size, arg.type, arg.chksizes
	# 	#client.create(arg)
	# if hasattr(args, 'remove'):
	# 	name = args.remove
	# 	client.remove(name)
	# if hasattr(args, 'list'):
	# 	name = args.list
	# 	client.list(name)
	# if hasattr(args, 'unmap'):
	# 	name = args.unmap
	# 	client.unmap(name)
	# if hasattr(args, 'mount'):
	# 	name = args.mount
	# 	client.mount(name)
	# if hasattr(args, 'unmount'):
	# 	name = args.unmount
	# 	client.unmount(name)

def test():
	client = Client.Client('192.168.0.12', 1234)
	args = ParseArgs(client)
	if isinstance(args, Object):
		print args.name
		print args.size
		print args.type
		print args.chunksizes, len(args.chunksizes)
		print args.subvolumes, len(args.subvolumes)
		print args.parameters, len(args.parameters)

if __name__=='__main__':
	test()