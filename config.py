import getopt, sys
import getopt, sys
import ConfigParser, string

def config(cfgdict):
	
	usage_print(cfgdict)
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
	opts, noOpt_args = getopt.getopt(sys.argv[1:], abbrevstring, verbose)
	# check whether the config file is specified in cmdline
	if cfgdict.has_key('cfgfile'):
		filename = cfgdict['cfgfile'][1]
	for opt,value in opts:
		if opt=='--cfgfile' or opt=='-f':
			filename = value
	# update the value of configuration parameters from config file, file name is specified in cfgdict['cfgfile']
	try:
		fp=open(filename,'rb')
		config = ConfigParser.ConfigParser()
		config.readfp(fp)
		fp.close()
		# print 'original value:'.ljust(20,' '), cfgdict
		section='default'
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
	# update the value of configuration parameters from cmdline
	# print 'opts:',opts
	# print 'args:',noOpt_args
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
	# print 'get value from command:'.ljust(20,' '), cfgdict
	ret_dict = dict([key,cfgdict[key][1]] for key in cfgdict)
	# print the usage message
	return ret_dict, noOpt_args

def usage_print(cfgdict, breadth1=5, breadth2=2, breadth3=50):
	for key in cfgdict:
		print ('  --'+key+',').ljust(breadth1,' ',),' ','-'+cfgdict[key][0].ljust(breadth2,' ',)
		longstr=cfgdict[key][2]+" (default:"+str(cfgdict[key][1])+')'
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
	helpmsg = '''group directories before files.
				augment with a --sort option, but any
				use of --sort=none (-U) disables grouping
			  '''
	default_cfgfile = './test.conf'

	cfgdict = {'MDS_IP':['M', '192.168.0.149', 'ip address of metadata server'], \
				'MDS_PORT':['m','6789','port of metadata server'], \
				'CHK_IP':['C', '192.168.0.149', helpmsg], \
				'CHK_PORT':['c', '3456', 'the port of chunk server'],\
				'enablexxx':['x',False,'enable x'],\
				'cfgfile':['f', default_cfgfile, 'name of the configuration file']}

	argudict, noOpt_args = config(cfgdict)
	# print argudict
	# print noOpt_args
	# for key in argudict:
	# 	print key, '=', argudict[key],' '

	# cfgdict must contain the cfgfile and the default value is specified.
