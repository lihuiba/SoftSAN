import getopt, sys
import getopt, sys
import ConfigParser, string

def config(cfgdict, filename, section='test'):
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
	# print 'get value from file:'.ljust(20,' '), cfgdict
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
	# print 'abbrevstring:', abbrevstring
	# print 'verbose:', verbose
	# try:
	opts, args = getopt.getopt(sys.argv[1:], abbrevstring, verbose)
	# except getopt.GetoptError, err:
	# 	print str(err) # will print something like "option -a not recognized"
	# 	sys.exit(2)
	# print 'opts:',opts
	# print 'args:',args
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
	# usage_print(cfgdict)
	return ret_dict

def usage_print(cfgdict, breadth1=5, breadth2=2, breadth3=40):
	for key in cfgdict:
		print ('  --'+key+',').ljust(breadth1,' ',),' ','-'+cfgdict[key][0].ljust(breadth2,' ',)
		indent_print(cfgdict[key][2], breadth3,breadth1+breadth2+10)
		
def indent_print(longstr,  breadth=30, indent=25):
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

if __name__ == '__main__':
	longstr = '''group directories before files.
				augment with a --sort option, but any
				use of --sort=none (-U) disables grouping
			  '''
	cfgdict = {'MDS_IP':['M','192.168.0.149','ip address of metadata server'], \
				'MDS_PORT':['m','6789','port of metadata server'], \
				'CHK_IP':['C','192.168.0.149',longstr], \
				'CHK_PORT':['c','3456',''],\
				'enablexxx':['x',False,'whether enable x']}
	cfgfile = '/home/hanggao/SoftSAN/test.conf'
	argudict = config(cfgdict, cfgfile)
	# for key in argudict:
	# 	print key, '=', argudict[key],' '
