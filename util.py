import messages_pb2 as msg
import guid as Guid

def gethostname(mdsip):
	'mdsip: the IP address of MDS'
	import socket
	s=socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	s.connect((mdsip, 12345))
	hostname=s.getsockname()[0]
	s.close()
	return hostname

class Object:
	def __init__(self, d=None):
		if isinstance(d, dict):
			self.__dict__=d

def message2object(message):
	"receive a PB message, returns its guid and a object describing the message"
	print message
	if not hasattr(message, 'ListFields'):
		return message
	fields=message.ListFields()
	rst=Object()
	for f in fields:
		name=f[0].name
		value=f[1]
		if isinstance(value, msg.Guid):
			value=Guid.toStr(value)
		else:
			listable=getattr(value, '__delitem__', None)
			if not listable:
				value=message2object(value)
			else:
				container=getattr(value, '_values', None)
				if container:
					value=[message2object(x) for x in container]
		setattr(rst, name, value)
	return rst

def object2message(object, message):
	if isinstance(object, dict):
		d=object
	else:
		d=object.__dict__
	for key in d.keys():
		if key.startswith('_'):
			continue
		try:
			setattr(message, key, d[key])
		except:
			pass


if __name__ == '__main__':
	print gethostname('localhost.localdomain')
