

def help():
	print '     velcome to SoftSAN 0.1     '.center(100, '-')

MDS_IP='192.168.0.12'
MDS_PORT=2340
CHK_IP='192.168.0.12'
CHK_PORT=6780
VGNAME=None


def usage():
	print 'Welcome to SoftSAN 0.1,  ChunkServer usage...'


def config(cfgdict, filename, section='test'):
	import ConfigParser
	config = ConfigParser.ConfigParser()
	config.readfp(open(filename,'rb'))
	# print 'original value:', cfgdict
	for key in cfgdict:
		try:
			cfgdict[key][1] = config.get(section,key)
		except:
			pass
	# print 'get value from file:'.ljust(30,' '), cfgdict
	# get value from command
	import getopt, sys
	abbrevstring='h'
	verbose = ['help']
	for key in cfgdict:
		verbose.append(key+'=')
		if cfgdict[key][0]==None:
			continue
		abbrevstring += (cfgdict[key][0]+':') 
		cfgdict[key][0] = '-'+cfgdict[key][0]
	print abbrevstring
	print verbose
	try:
		opts, args = getopt.getopt(sys.argv[1:], abbrevstring, verbose)
	except getopt.GetoptError, err:
		print str(err) # will print something like "option -a not recognized"
		print 'type -h or --help to get help'
		sys.exit(2)
	# print opts
	for v in verbose[1:]:
		v = '--'+v
		v = v[:-1]
	for o,a in opts:
		if o in ('-h','--help'):
			help()
			exit()
		for key in cfgdict:
			if o in ('--'+key, cfgdict[key][0]):
				cfgdict[key][1] = a
	# print 'get value from command:'.ljust(30,' '), cfgdict
	# check the value, make sure no value equals zero
	ret_dict = {}
	for key in cfgdict:
		if cfgdict[key][1]==None:
			print 'None value is illegal:', key
			exit()
		ret_dict[key] = cfgdict[key][1]
	return ret_dict

if __name__ == '__main__':
	cfgdict = {'MDS_IP':['M','192.168.0.149'], 'MDS_PORT':['m','6789'], \
				'CHK_IP':['C','192.168.0.149'], 'CHK_PORT':['c','3456']}
	cfgfile = '/home/hanggao/SoftSAN/test.conf'
	argudict = config(cfgdict, cfgfile)
	for key in argudict:
		print key, '=', argudict[key],' '
