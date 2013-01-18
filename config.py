import getopt, sys
import ConfigParser, string

def config(cfgstruct, cfgfile='softsan.conf', section='default'):
	cfgdict=dict([item[0], list(item[1:])] for item in cfgstruct)
	# get value from command
	abbrevstring=''
	verbose = []
	for key in cfgdict:
		if isinstance(cfgdict[key][1], bool):
			verbose.append(key)
			abbrevstring += cfgdict[key][0]
		else:
			verbose.append(key+'=')
			abbrevstring += (cfgdict[key][0]+':')
	# print sys.argv
	try:
		opts, noOpt_args = getopt.gnu_getopt(sys.argv[1:], abbrevstring, verbose)
	except Exception,e :
		print e
	# check whether the config file is specified in cmdline
	if cfgdict.has_key(cfgfile):		#if app allows the user to specify a conf file name
		filename = cfgdict[cfgfile][1]	#the default file name
		cfgfilefull='--'+cfgfile
		cfgfileshort='-'+cfgdict[cfgfile][0]
		for opt,value in opts:
			if opt==cfgfilefull or opt==cfgfileshort:	#if the user does specify a file name
				filename = value
				break
	else:
		filename = cfgfile 				#app specify a certain conf file name

	# update the value of configuration parameters from config file, file name is specified in cfgdict['cfgfile']
	try:
		fp=open(filename,'rb')
		config = ConfigParser.ConfigParser()
		config.readfp(fp)
		fp.close()
		# print 'original value:'.ljust(20,' '), cfgdict
		for key in cfgdict:
			try:
				value = config.get(section,key)
				if value.lower()=='true':
					cfgdict[key][1]=True
				elif value.lower()=='false':
					cfgdict[key][1]=False
				else:
					cfgdict[key][1]=value
			except:
				pass
	except:
		pass
	for o,a in opts:
		o = o.lstrip('-')
		if o in cfgdict:
			item=cfgdict[o]
		else:
			for key in cfgdict:
				item=cfgdict[key]
				if o==item[0]: break
			assert o==item[0]
		if isinstance(item[1], bool):
			item[1]=True
		else:
			item[1] = a
	ret_dict = dict([key,cfgdict[key][1]] for key in cfgdict)
	return ret_dict, noOpt_args

def usage_print(cfgstruct, breadth1=5, breadth2=2, breadth3=50):
	for item in cfgstruct:
		print ('-'+item[1]+',').ljust(breadth2,' '), ('  --'+item[0]).ljust(breadth1,' ')
		longstr=item[3]+" (default: "+str(item[2])+')'
		indent_print(longstr, breadth3, breadth1+breadth2+20)

def indent_print(longstr,  breadth=50, indent=15):
	longstr = string.replace(longstr,'\n',' ')
	longstr = string.replace(longstr, '\t',' ')
	lst = longstr.split()
	line = ''
	head = ''
	for i in range(indent):
		head += ' '
	for word in lst:
		l = len(word)
		if (len(line)+l)<breadth:
			line += (word+' ')
		else:
			print head, line
			line = word+' '
	print head,line

if __name__ == '__main__':
	import util,config

	cfgstruct = (\
	('address',			'a',	'0.0.0.0',			'IP address to bind'), \
	('port',			'p',	0x2121,				'TCP port to bind'), \
	('mdsaddress',		'M',	'127.0.0.1',			'meta data server address'), \
	('mdsport',			'm',	0x26505,				'meta data server TCP port'), \
	('vg',				'g',	'SANGroup',			'name of volume group for use exclusively by SoftSAN'),\
	('volprefix',		'z',	'lv_softsan_',		'prefix of volume name'),\
	('logging-level',	'l',	'info',				'logging level, can be "debug", "info", "warning", "error" or "critical"'), \
	('logging-file',	'f',	'stdout',			'logging appends to this file'),\
	('config',			'c',	'Chunkserver.conf',	'config file'),\
	('help',			'h',	False,				'this help'),\
	)

	configure, noOpt_args = config(cfgstruct)
	print configure,'00000000000000000'
	if configure['help']==True:
		config.usage_print(cfgstruct)
		exit(0)
	confobj = util.Object(configure)
	
	