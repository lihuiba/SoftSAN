#!/usr/bin/python
import Client, config
from argparse import ArgumentParser as ArgParser
from mds import Object
import sys, os


CHUNKSIZE = 64

def ParseTable(words):
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
		print >>sys.stderr, 'Too few table arguments'
		exit(-1)
	if isinstance(words[0], str)==False or words[1].isdigit()==False:
		print >>sys.stderr, 'incorrect table format'
		exit(-1)
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
			print >>sys.stderr, 'Unrecognized argument: ' + words[argc]
			exit(-1)
		argc += 1
		if argc >= num:
			return arg
	if words[argc].isdigit():
		num_vol = int(words[argc])
		argc += 1
		if argc >= num:
			chksize = arg.size/num_vol
			if chksize < 4:
				print >>sys.stderr, 'too many chunks or chunk size too samll'
				exit(-1)
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
				print >>sys.stderr, 'chunk size summary is larger than voluem size'
				exit(-1)
			remain = arg.size-totsize
			while remain > CHUNKSIZE:
				arg.chunksizes.append(CHUNKSIZE)
				remain -= CHUNKSIZE
			if remain > 0:
				arg.chunksizes.append(remain)
			return arg
		else:
			if num-argc != num_vol:
				print >>sys.stderr, 'there are {0} volumes given'.format(num-argc)
				exit(-1)
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

def isExclusive(args):
	flag = 0
	if args.create != '':
		flag += 1
	if args.remove != '':
		flag += 1
	if args.split != '':
		flag += 1
	if args.info != '':
		flag += 1
	if args.mount != '':
		flag += 1
	if args.unmount != '':
		flag += 1
	if flag == 0 or flag > 1:
		return False
	return True

def ParseArg(client):
	commands=(\
		('create',  'c',  '',        'create a volume'),\
		('params',  'p',  '',		 'volume parameters'),\
		('remove',  'r',  '',		 'remove a volume'),\
		('info',    'i',  '',		 'list object information'),\
		('split',   's',  '',        'split a volume into subvolumes'),\
		('mount',   'm',  '',        'mount a volume'),\
		('unmount', 'u',  '',	     'unmount a volume'),\
		('help',	'h',  False,	 'this help')
	)

	argstruct=(\
		('mdsaddress',		'a',		'',					'metadata server address'),\
		('mdsport',			'p',		0x6789,				'metadata server port'),\
		('logging-level',	'l',		'info',				'logging level, can be "debug", "info", "warning", "error" or "critical"'), \
		('config',			'c',		'softsan-cli.conf',	'config file'),\
	)

	config,_ = config.config(argstruct, 'softsan-cli.conf')
	if config['mdsaddress'] != True:
		print >>sys.stderr,  'must specifiy metadata server address first'
		exit(-1)
	if config['mdsport'] != True:
		print >>sys.stderr, 'must specifiy metadata server port first'
		exit(-1)
	if not config['logging-level'] in util.str2logginglevel:
		print >>sys.stderr, 'invalid logging level "{0}"'.format(level)
		exit(-1)
	
	cmds, remains = config.config(commands, None)
	cmds = Object(cmds)

	if not isExclusive(cmds):
		print >>sys.stderr, 'Invalid argument'
		exit(-1)

	print remains
	print cmds.params

	if cmds.create != '':
		name = cmds.create
		words = [name]
		words.extend(remains)
		data = ParseTable(words)
		if isinstance(data, Object) == False:
			print 'table format error'
			return None
		if cmds.params:
			strsize = int(cmds.params)
			if strsize != 256 and strsize != 512 and strsize != 1024 and strsize != 2048:
				print 'stripe size only can be: 256/512/1024/2048'
				return None
			data.parameters.append(cmds.params)
		func = 'Create'
	if cmds.remove != '':
		data = cmds.remove
		func = 'Delete'
	if cmds.info != '':
		data = cmds.info
		func = 'Info'
	if cmds.split != '':
		data = cmds.split
		func = 'Split'
	if cmds.mount != '':
		data = cmds.mount
		func = 'Mount'
	if cmds.unmount != '':
		data = cmds.unmount
		func = 'Unmount'
	if isinstance(data,str):
		if len(remains)>0:
			print 'Invalid argument'
			return None
		error = CheckName(client.mds, data)
		if error != '':
			print error
			return None

	getattr(client, func+'Volume')(data)
	client.Clear()
	return data

def test():
	sys.argv = 'softsan-cli.py --create no1 60 striped 2'.split()
	sys.argv = 'softsan-cli.py --info no1'.split()
	sys.argv = 'softsan-cli.py --unmount no1'.split()
	sys.argv = 'softsan-cli.py --mount no1'.split()
	sys.argv = 'softsan-cli.py --remove no1'.split()
	client = Client.Client('192.168.0.12', 6789)
	args = ParseArg(client)
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
	test()