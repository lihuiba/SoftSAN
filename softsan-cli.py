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
			print 'Unrecognized argument: ' + words[argc]
			return None
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
	ArgsDict=(\
		('create',  'c',  '',        		'create a volume'),\
		('params',  'p',  '',		 		'volume parameters'),\
		('remove',  'r',  '',				'remove a volume'),\
		('info',    'i',  '',		 		'list object information'),\
		('split',   's',  '',       		'split a volume into subvolumes'),\
		('mount',   'm',  '',        		'mount a volume'),\
		('unmount', 'u',  '',	     		'unmount a volume'),\
		('cfgfile', 'f',  './tests.conf',   'configuation file of SoftSAN')\
	)

	ret, remains = config.config(ArgsDict)
	args = Object(ret)

	if not isExclusive(args):
		print 'Invalid argument'

	print remains
	print args.params

	if args.create != '':
		name = args.create
		words = [name]
		words.extend(remains)
		data = ParseTable(words)
		if isinstance(data, Object) == False:
			print 'table format error'
			return None
		if args.params:
			strsize = int(args.params)
			if strsize != 256 and strsize != 512 and strsize != 1024 and strsize != 2048:
				print 'stripe size only can be: 256/512/1024/2048'
				return None
			data.parameters.append(args.params)
		func = 'Create'
	if args.remove != '':
		data = args.remove
		func = 'Delete'
	if args.info != '':
		data = args.info
		func = 'Info'
	if args.split != '':
		data = args.split
		func = 'Split'
	if args.mount != '':
		data = args.mount
		func = 'Mount'
	if args.unmount != '':
		data = args.unmount
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