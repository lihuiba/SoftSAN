import mds, logging, config, util

cfgfile = '/home/hanggao/SoftSAN/test.conf'
cfgdict = (\
	('address',			'a',	'0.0.0.0',	'ip address to bind'), \
	('port',			'p',	0x6789,		'tcp port to bind'), \
	('logging-level',	'l',	'info',		'logging level, can be "debug", "info", "warning", "error" or "critical"'), \
	('config',			'c',	cfgfile,	'config file'),\
	('help',			'h',	False,		'this help'),\
)

#fixme: cfgfile itself can be specifid in the cmdline
#fixme: the 2nd arg speicify the key in the dict to config file path and name. if it's found in the dict, then filename is dict[key], otherwise filename is key.
#fixme: cfgdict should not be modified, so that it can be used in following usage_print()
#fixme: usage_print() should print the default values, too.
try:
	configure,_ = config.config(cfgdict, 'config')
except:
	pass

if configure['help']==True:
	print """Meta-Data Server of SoftSAN
Usage: mds-cli [options]
options:"""
	config.usage_print(cfgdict)
	exit(0)

exit(0)

logging.basicConfig(level=info)
loglevel=configure['logging-level']
if not level in util.str2logginglevel:
	import sys
	print >>sys.stderr, 'invalid logging level "{0}"'.format(level)
	exit(-1)
logging.basicConfig(level=loglevel)

