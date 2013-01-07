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
	for key in d:
		if key.startswith('_'):
			continue
		value=d[key]
		if isinstance(value, list):
			mfield=getattr(message, key)
			appender = (lambda x : mfield.append(x)) if hasattr(mfield, 'append') \
				  else (lambda x : object2message(x, mfield.add()))
			for item in value:
				appender(item)
		else:
			try:
				setattr(message, key, value)
			except:
				pass

class Pool:
	def __init__(self, constructor, destructor):
		self.pool={}
		self.ctor=constructor
		self.dtor=destructor
	def get(self, *key):
		if key in self.pool:
			return self.pool[key]
		value=self.ctor(*key)
		self.pool[key]=value
		return value
	def dispose(self):
		for key in self.pool.keys():
			self.dtor(self.pool[key])
			del self.pool[key]

def testPool():
	class Stub:
		def __init__(self,a,b,c,d):
			self.key=(a,b,c,d)
			print 'constructing ', self.key
		def close(self):
			print 'destructing ', self.key
	pool=Pool(Stub, Stub.close)
	pool.get(10,20,3,2)
	pool.get(32,1.2312,56,32)
	pool.get('asdf', 'jkl;', 32, 3.13)
	pool.dispose()	

	class Socket:
		def __init__(self,endpoint):
			self.key=endpoint
			print 'constructing ', self.key
		def close(self):
			print 'destructing ', self.key
	pool=Pool(lambda *args : Socket(args), Socket.close)
	pool.get(10,20)
	pool.get(32,1.2312)
	pool.get('asdf', 'jkl;')
	pool.dispose()


if __name__ == '__main__':
	#print gethostname('localhost.localdomain')
	testPool()
