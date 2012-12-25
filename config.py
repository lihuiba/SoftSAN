import getopt, sys
import getopt, sys
import ConfigParser

def config(cfgdict, filename, section='test'):
	import ConfigParser
	config = ConfigParser.ConfigParser()
	config.readfp(open(filename,'rb'))
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
		if cfgdict[key][1]==True or cfgdict[key][1]==False:
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
		for i in range(2):
			if o[0]=='-':
				o = o[1:]
		for key in cfgdict:
			if o in (key, cfgdict[key][0]):
				if cfgdict[key][1]==False:
					cfgdict[key][1]=True
				else:
					cfgdict[key][1] = a
	# print 'get value from command:'.ljust(20,' '), cfgdict
	# check the value, make sure no value equals zero
	ret_dict = {}
	for key in cfgdict:
		ret_dict[key] = cfgdict[key][1]
	return ret_dict

if __name__ == '__main__':
	cfgdict = {'MDS_IP':['M','192.168.0.149'], 'MDS_PORT':['m','6789'], \
				'CHK_IP':['C','192.168.0.149'], 'CHK_PORT':['c','3456'],'enablexxx':['x',False]}
	cfgfile = '/home/hanggao/SoftSAN/test.conf'
	argudict = config(cfgdict, cfgfile)
	for key in argudict:
		print key, '=', argudict[key],' '
