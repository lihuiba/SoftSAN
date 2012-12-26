import getopt, sys
import getopt, sys
import ConfigParser

def config(cfgdict, filename, section='test'):
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
	try:
		opts, args = getopt.getopt(sys.argv[1:], abbrevstring, verbose)
	except getopt.GetoptError, err:
		print str(err) # will print something like "option -a not recognized"
		sys.exit(2)
	# print 'opts:',opts
	# print 'args:',args
	for o,a in opts:
		o = o.lstrip('-')
		if o in cfgdict:
			item=cfgdict[o]
		else:
			for key in cfgdict:
				item=cfgdict[key]
				if o==item[1]: break
			assert o==item[1]
		if isinstance(item[1], bool):
			item[1]=True
		else:
			item[1] = a
	# print 'get value from command:'.ljust(20,' '), cfgdict
	# print the help message
	print 'Usage:'
	print ' ',
	print './chunkserver [Options] [value]'
	print
	print 'Options:'
	for key in cfgdict:
		print ('  --'+key+',').ljust(18,' ',),' ','-'+cfgdict[key][0].ljust(2,' ',),cfgdict[key][2]
	print


	ret_dict = dict([key,cfgdict[key]] for key in cfgdict)
	return ret_dict

if __name__ == '__main__':
	cfgdict = {'MDS_IP':['M','192.168.0.149','ip address of metadata server'], \
				'MDS_PORT':['m','6789','port of metadata server'], \
				'CHK_IP':['C','192.168.0.149','ip address of chunkserver server ************************************************'], \
				'CHK_PORT':['c','3456',''],\
				'enablexxx':['x',False,'whether enable x']}
	cfgfile = '/home/hanggao/SoftSAN/test.conf'
	argudict = config(cfgdict, cfgfile)
	# for key in argudict:
	# 	print key, '=', argudict[key],' '
