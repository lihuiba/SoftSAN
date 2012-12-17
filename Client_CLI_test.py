import rpc, logging
import messages_pb2 as msg
import client_messages_pb2 as clmsg
import mds, ChkSvr_mock
import gevent.socket
from Client_test import Client
import guid as Guid
import scandev
import block.dm as dm

logging.basicConfig(level=logging.DEBUG)

socket=gevent.socket.socket()
socket.connect(('192.168.0.12', 6767))


guid=msg.Guid()
guid.a=10
guid.b=22
guid.c=30
guid.d=40
MI = {}
req = getattr(clmsg, 'NewVolume_Request', None)
res = getattr(clmsg, 'NewVolume_Response', type(None))
MI['NewVolume'] = (req, res)
req = getattr(clmsg, 'DeleteVolume_Request', None)
res = getattr(clmsg, 'DeleteVolume_Response', type(None))
MI['DeleteVolume'] = (req, res)
print MI
stub=rpc.RpcStub(guid, socket, Client, MI)
arg = clmsg.NewVolume_Request()
arg.volume_name = 'ssVolume2'
arg.volume_size = 10240
arg.volume_type = ''
arg.volume_type = 'striped'
arg.striped_size = 0
ret = stub.callMethod('NewVolume', arg)
maps = dm.maps()
if len(maps) is not 0:
	print 'The new device is :' + maps[0].name
else:
	print 'Client_CLI_test: call method failed'

arg = clmsg.DeleteVolume_Request()
arg.volume_name = 'ssVolume2'
ret = stub.callMethod('DeleteVolume', arg)