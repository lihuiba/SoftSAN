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
	import guid as Guid, messages_pb2 as msg
	if not hasattr(message, 'ListFields'):
		return message
	fields=message.ListFields()
	rst=Object()
	for f in fields:
		name=f[0].name
		value=f[1]
		if isinstance(value, msg.Guid):
			value=Guid.toStr(value)
		elif hasattr(value, 'ListFields'):
			value=message2object(value, '')
		elif hasattr(value, '_values'):
			value=[message2object(x) for x in value._values]
		else:
			pass  #should be a native value like str, int, float, ...			
		setattr(rst, name, value)
	return rst

def object2message(object, message):
	d = object if isinstance(object, dict) else object.__dict__
	for key in d.keys():
		if key.startswith('_'):
			continue
		value=d[key]
		try:
			if isinstance(value, list):
				appender = (lambda x : repeated.append(x)) if hasattr(message[key], append) \
					  else (lambda x : object2message(x, repeated.add()))
				for item in value:
					appender(item)
			else:
				setattr(message, key, d[key])
		except:
			pass

if __name__ == '__main__':
	print gethostname('localhost.localdomain')
