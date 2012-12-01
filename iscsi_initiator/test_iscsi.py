import os
import libiscsi
import scandev

# check the following functions
# libiscsi.discover_sendtargets  | iscsi discovery
# node.login()                   | iscsi login
# node.logout()                  | iscsi logout 
# scandev.get_blockdev_by_targetname()  | get block device name

#discover nodes from ipaddress '192.168.0.12'
nodelist = libiscsi.discover_sendtargets('192.168.0.12')
if nodelist is not None :
	print nodelist[0].address
	print nodelist[0].name
else:
	print 'no node found!'

#login node 0
nodelist[0].login()

#get the locak blockdevicename of this target
print scandev.get_blockdev_by_targetname(nodelist[0].name)

#logout
nodelist[0].logout()
