MDS_IP=None
MDS_PORT=None
CHK_IP=None
CHK_PORT=None
VGNAME=None

def usage():
	print 'Welcome to SoftSAN 0.1,  ChunkServer usage...'
	print 'python ChunkServer.py -a -b -c -d -n -h'
	print '-a ip of meatadata server(192.168.0.149) '
	print '-b port of meatadata server(1234) '
	print '-c ip of chunkserver(192.168.0.149)'
	print '-d port of chunkserver(5678)'
	print '-n name of VolGroup '

def config_from_cmd():
	global MDS_IP, MDS_PORT, CHK_IP, CHK_PORT, VGNAME
	import getopt, sys
	verbose = ["mdsip=", "mdsport=", "chksvrip=", "chksvrport=", "vgname=", "verbose", "help"]
	abbrev = "a:b:c:d:n:v:h"
	try:
		opts, args = getopt.getopt(sys.argv[1:], abbrev, verbose)
	except getopt.GetoptError, err:
		print str(err) # will print something like "option -a not recognized"
		usage()
		sys.exit(2)
	mdsip=None
	mdsport=None
	chksvrip=None
	chksvrport=None
	vgname=None
	for o, a in opts:
		if o == ("-v", "--verbose"):
			print 'SoftSAN 0.1 ......'
			sys.exit()
		elif o in ("-h", "--help"):
			usage()
			sys.exit()
		elif o in ("-a", "--mdsip"):
			mdsip = a
		elif o in ("-b", "--mdsport"):
			mdsport = a
		elif o in ("-c", "--chksvrip"):
			chksvrip = a
		elif o in ("-d", "--chksvrport"):
			chksvrport = a
		elif o in ("-n", "--name"):
			vgname = a
		else:
			assert False, "unhandled option"
	if mdsip != None:
		MDS_IP=mdsip
	if mdsport != None:
		MDS_PORT = int(mdsport)
	if chksvrip != None:
		CHK_IP = chksvrip
	if chksvrport != None:
		CHK_PORT = int(chksvrport)
	if vgname != None:
		VGNAME = vgname
	print 'config from command >>> ', 'mdsip:', MDS_IP, 'mdsport:',MDS_PORT, 'vgname:',VGNAME, 'chksvrip:', CHK_IP, 'chksvrport:', CHK_PORT

def config_from_file(filename='/home/hanggao/SoftSAN/test.conf'):
	global MDS_IP, MDS_PORT, CHK_IP, CHK_PORT, VGNAME
	import ConfigParser
	config = ConfigParser.ConfigParser()
	config.readfp(open(filename,'rb'))
	if MDS_IP==None:
		try:
			mdsip = config.get('global', 'mdsip')
			MDS_IP = mdsip
		except:
			pass
	if MDS_PORT==None:
		try:
			mdsport = config.get('global', 'mdsport')
			MDS_PORT = int(mdsport)
		except:
			pass
	if VGNAME==None:
		try:
			vgname = config.get('global', 'vgname')
			VGNAME = vgname
		except:
			pass
	if CHK_IP==None:
		try:
			chksvrip = config.get('global', 'chksvrip')
			CHK_IP = chksvrip
		except:
			pass
	if CHK_PORT==None:
		try:
			chksvrport = config.get('global', 'chksvrport')
			CHK_PORT = int(chksvrport)
		except:
			pass
	print 'config from file >>> ', 'mdsip:', MDS_IP, 'mdsport:',MDS_PORT, 'vgname:',VGNAME, 'chksvrip:', CHK_IP, 'chksvrport:', CHK_PORT

def default_config():
	global MDS_IP, MDS_PORT, CHK_IP, CHK_PORT, VGNAME
	if MDS_IP==None:
		MDS_IP='192.168.0.149'
	if MDS_PORT==None:
		MDS_PORT=2340
	if CHK_IP==None:
		CHK_IP='192.168.0.149'
	if CHK_PORT==None:
		CHK_PORT=6780
	if VGNAME==None:
		VGNAME='VolGroup'
	
	print 'config from default >>> ', 'mdsip:', MDS_IP, 'mdsport:',MDS_PORT, 'vgname:',VGNAME, 'chksvrip:', CHK_IP, 'chksvrport:', CHK_PORT

def softsan_config(filename='/home/hanggao/SoftSAN/test.conf'):
	config_from_cmd()
	config_from_file(filename)
	default_config()