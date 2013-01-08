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

	if num < 2:
		return None
	if isinstance(words[0], str)==False or isinstance(words[1], int)==False:
		print tableFormat
		return None
	arg.name = words[0]
	arg.size = words[1]

	if num >= 3:
		if words[2].isdigit() == False:
			if words[2] == 'striped':
				arg.type = 'striped'
			elif words[2] == 'linear':				
				arg.type = 'linear'
			elif words[2] == 'gfs':
				arg.type = 'gfs'
			else:
				error = 'Unrecognized argument: ' + words[2]
		#else:
			

	if isinstance(word[3], int):
		totsize = 0
		for i in range(3, len(words)):
			if not isinstance(words[i], int):
				print tableFormat
				return arg
			totsize += words[i]
			arg.chunksizes.append(words[i])
		remain = words[1]-totsize
		if(remain < 0):
			print 'chunk size summary is not equal to volume size'
			return arg
		while remain > CHUNKSIZE:
			arg.chunksizes.append(CHUNKSIZE)
			remain -= CHUNKSIZE
		if remain > 0:
			arg.chunksizes.append(remain)

	if isinstance(word[3], str):
		for i in range(3, len(words)):
			if not isinstance(words[i], str):
				print tableFormat
				return arg
			arg.subvolumes.append(words[i])

	return arg

def CheckName(client, name):
	if name == '':
		return ''
	if not isinstance(name, str):
		return  'Name should be a string!'
	if client.ReadVolumeInfo(name) == None:
		return 'Volume ' + name + ' is not exsit!'
	return ''

def ParseArgs(client):
	parser = ArgParser()
	subparsers = parser.add_subparsers()
	# create
	parser_create = subparsers.add_parser('create', help='create a volume')
	parser_create.add_argument('table', nargs='+', help='table to build a volume')
	parser_create.add_argument('--file', dest='path', help='table file')
	#parser_create.add_argument('--num', '-n', nargs='*', dest='subvolumes', help='chunk or subvolume')
	parser_create.set_defaults(func='Create')
	# remove
	parser_remove = subparsers.add_parser('remove', help='remove a volume')
	parser_remove.add_argument('volume_name', help='volume to be removed')
	parser_create.set_defaults(func='Delete')
	# list
	parser_list = subparsers.add_parser('list', help='show volume info')
	parser_list.add_argument('volume_name', help='volume to be removed')
	parser_create.set_defaults(func='List')
	# split
	parser_split = subparsers.add_parser('split', help='split a volume')
	parser_split.add_argument('volume_name', help='volume to be removed')
	parser_create.set_defaults(func='Split')
	# mount
	parser_mount = subparsers.add_parser('mount', help='mount a volume')
	parser_mount.add_argument('volume_name', help='volume to be removed')
	parser_create.set_defaults(func='Mount')
	# unmount
	parser_unmount = subparsers.add_parser('unmount', help='unmount a volume')
	parser_unmount.add_argument('volume_name', help='volume to be removed')
	parser_create.set_defaults(func='Unmount')

	args = parser.parse_args()
	if args.func == 'Create':
		data = ParseLine(args.table)
	else:
		data = args.volume_name
		error = CheckName(data)
		if not error == '':
			print error
			return None
		
	getattr(client, args.func+'Volume')(data)


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
