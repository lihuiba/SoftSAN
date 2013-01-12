import ChunkServer, gevent.server, rpc, gevent
import logging, config, util, sys, os.path

header="""Chunk Server of SoftSAN
Usage: chkserv-cli [options]
options:"""

cfgstruct = (\
	('address',			'a',	'0.0.0.0',	'ip address to bind'), \
	('port',			'p',	'1821',		'tcp port to bind'), \
	('mds_ip',			'M',	'192.168.0.149',	'mds ip address'), \
	('mds_port',		'm',	'6789',		'mds port'), \
	('logging-level',	'l',	'info',		'logging level, can be "debug", "info", "warning", "error" or "critical"'), \
	('config',			'c',	'Chksevr.conf',	'config file'),\
	('help',			'h',	False,		'this help'),\
)

try:
	configure,_ = config.config(cfgstruct, 'config')
except:
	pass

if configure['help']==True:
	print header
	config.usage_print(cfgstruct)
	exit(0)

loglevel=configure['logging-level']
if not loglevel in util.str2logginglevel:
	print >>sys.stderr, 'invalid logging level "{0}"'.format(level)
	exit(-1)
loglevel=util.str2logginglevel[loglevel]
logging.basicConfig(level=loglevel)

server=ChunkServer.ChunkServer()
print configure['mds_ip'], int(configure['mds_port']), '_____________________________'
gevent.spawn(ChunkServer.heartBeat, server=server, ip=configure['mds_ip'], port=int(configure['mds_port']))
service=rpc.RpcService(server)
framework=gevent.server.StreamServer((configure['address'],int(configure['port'])), service.handler)
framework.serve_forever()