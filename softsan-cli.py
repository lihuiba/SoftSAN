#!/usr/bin/python
import Client, config, util
import sys, os, cmd
from mds import Object


CHUNKSIZE = 64

commands=(\
	('name',	'n',	'',			'create a volume'),\
	('params',  'p',	'',			'volume parameters'),\
	('size',	's',	'',			'remove a volume'),\
	('count',	'c', 	'0',		'list object information'),\
	('type',	't', 	'linear',	'split a volume into subvolumes'),\
	('tree',	'', 	'0',		'list object information'),\
)
argstruct=(\
	('mdsaddress',		'a',		'',					'metadata server address'),\
	('mdsport',			'p',		0x6789,				'metadata server port'),\
	('logging-level',	'l',		'info',				'logging level, can be "debug", "info", "warning", "error" or "critical"'), \
	('config',			'c',		'softsan-cli.conf',	'config file'),\
)

def ParseTable(args, subvolumes):
	ret = Object()
	ret.name = ''
	ret.size = 0
	ret.type = ''
	ret.chunksizes = []
	ret.subvolumes = []
	ret.parameters = []
	tableFormat = 'name size [type] [num] [sizes|volumenames]'
	if args.name == '':
		print >>sys.stderr, 'volume name must be specified'
		return None
	ret.name = args.name
	if args.size.isdigit() == False:
		print >>sys.stderr, 'volume size must be digit'
		return None
	if args.size != '':
		ret.size = int(args.size)
	if args.type != 'linear' and args.type != 'striped' and\
		args.type != 'mirror' and args.type != 'gfs':
		print >>sys.stderr, 'volume type can only be linear/striped/mirror/gfs'
		return None
	if args.params != '':
		if args.type != 'striped':
			print 'warning: not need parameters'
		else:
			strsize = int(args.params)
			if strsize != 256 and strsize != 512 and strsize != 1024 and strsize != 2048:
				print >>sys.stderr, 'stripe size only can be: 256/512/1024/2048'
				return None
			ret.parameters.append(args.params)
	if args.count.isdigit() == False:
		print >>sys.stderr, 'count must be digit'
		return None
	count = int(args.count)
	if count < 0:
		print >>sys.stderr, 'count can not be negative'
		return None
	if count == 0:
		if ret.type == 'linear' or ret.type == 'gfs':
			chksize = CHUNKSIZE
			totsize = ret.size
			while totsize > chksize:
				ret.chunksizes.append(chksize)
				totsize -= chksize
			if totsize > 0:
				ret.chunksizes.append(totsize)
		else:
			count = 2
	if count > 0:
		num = len(subvolumes)
		if num == 0:
			chksize = ret.size/count
			if chksize < 4:
				print >>sys.stderr, 'too many chunks, single chunk size too samll'
				return None
			totsize = ret.size
			while totsize > chksize:
				ret.chunksizes.append(chksize)
				totsize -= chksize
			if totsize > 0:
				ret.chunksizes.append(totsize)
			return ret
		if num != count:
			print >>sys.stderr, 'incorrect arguments count'
			return None
		if subvolumes[0].isdigit() == True:
			totsize = 0
			for i in range(0, count):
				ret.chunksizes.append(int(subvolumes[i]))
				totsize += int(int(subvolumes[i]))
			if totsize > ret.size:
				print >>sys.stderr, 'chunk size summary is larger than voluem size'
				return None
			if totsize < ret.size:
				print >>sys.stdwar, 'warning: chunk size summary is less than voluem size'
			remain = ret.size-totsize
			while remain > CHUNKSIZE:
				ret.chunksizes.append(CHUNKSIZE)
				remain -= CHUNKSIZE
			if remain > 0:
				ret.chunksizes.append(remain)
		else:
			ret.subvolumes = subvolumes
	return ret

def ParseCMD(action, cmdline):
	cmdline = 'softsan '+cmdline
	sys.argv = cmdline.split()
	print sys.argv
	cmds, remains = config.config(commands, None)
	print cmds
	print remains
	cmds = Object(cmds)

	if action == 'create':
		name = cmds.name
		data = ParseTable(cmds, remains)
		if isinstance(data, type(None)):
			return
		remains = []
		func = 'Create'
	elif action == 'remove':
		data = cmds.name
		func = 'Delete'
	elif action == 'list' or action == 'ls':
		data = Object()
		data.tree = cmds.tree
		data.name = cmds.name
		func = 'List'
	elif action == 'split':
		data = cmds.name
		func = 'split'
	elif action == 'mount':
		data = cmds.name
		func = 'Mount'
	elif action == 'unmount':
		data = cmds.name
		func = 'Unmount'

	if len(remains)>0:
		print >>sys.stderr, 'Invalid arguments: ',
		for i in remains:
			print i,
		print ''
		return

	#getattr(client, func+'Volume')(data)


def ParseArg():
	conf, ss = config.config(argstruct, 'softsan-cli.conf')
	
	if not conf['logging-level'] in util.str2logginglevel:
		print >>sys.stderr, 'invalid logging level "{0}"'.format(level)
		exit(-1)

	#client = Client.Client(conf['mdsaddress'], conf['mdsport'])
	cli = CLI()
	cli.cmdloop()
	#client.Clear()


class CLI(cmd.Cmd):
	def __init__(self):
		cmd.Cmd.__init__(self)
		self.prompt = 'softsan# '

	def do_create(self, line):
		ParseCMD('create', line)

	def help_test(self):
		print 'create a new volume'

	def do_remove(self, line):
		ParseCMD('remove', line)

	def help_remove(self):
		print 'remove volume'

	def do_list(self, line):
		ParseCMD('list', line)

	def help_list(self):
		print 'list volume'

	def do_ls(self, line):
		ParseCMD('list', line)

	def help_ls(self):
		print 'list volume'

	def do_split(self, line):
		ParseCMD('split', line)

	def help_split(self):
		print 'split a volume into subvolume(s)'

	def do_mount(self, line):
		ParseCMD('mount', line)

	def help_mount(self):
		print 'mount volume'

	def do_unmount(self, line):
		ParseCMD('unmount', line)

	def help_unmount(self):
		print 'unmount volume'

	def do_exit(self, line):
		exit(0)

	def help_exit(self):
		print 'exit program'

def test():
	sys.argv = 'softsan-cli.py -a 192.168.0.12'.split()
	# sys.argv = 'create no1 60 striped 2'.split()
	# sys.argv = 'softsan-cli.py --info no1'.split()
	# sys.argv = 'softsan-cli.py --unmount no1'.split()
	# sys.argv = 'softsan-cli.py --mount no1'.split()
	# sys.argv = 'softsan-cli.py --remove no1'.split()
	args = ParseArg()
	if isinstance(args, Object):
		print args.name
		print args.size
		print args.type
		print args.chunksizes, len(args.chunksizes)
		print args.subvolumes, len(args.subvolumes)
		print args.parameters, len(args.parameters)
	else:
		print args

if __name__=='__main__':
	#test()
	ParseArg()