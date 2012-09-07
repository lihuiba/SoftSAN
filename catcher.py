import inspect
import logging


# class Wrapper:
# 	_target=None
# 	def __init__(self, target):
# 		_target=target
# 	def __getattr__(self, name):
# 		ret=self.find(_target.__dict__, name) or self.find(_target.__class__.__dict__, name)
# 		return ret
# 	@staticmethod
# 	def find(dict, name):
# 		if dict[name] and 

def AttachCatcher(obj, functions):
	if functions==None:
		functions=[x in dir(obj) if inspect.isroutine(x) and not x.startswith('__') ]
	# if functions is string:
	# 	functions=[functions]
	for func in functions:
		f=getattr(obj, func)
		def wrapper(*args, **kwargs):
			try:
				ret=f(*args, **kwargs)
				return ret
			except Error as e:
				logging.error('exception caught: '+str(e))
		setattr(obj, func, wrapper)


if __name__=="__main__":
	class kkkk:
		def abcd(a,b):
			print "a,b:", a,b
			raise IOError
		def asdf(a,s):
			print a+s
	k=kkkk()
	kc=AttachCatcher(k, ["abcd", "asdf"])
	print(kc.__dict__)
	f=kc.abcd
	f(1,2)
	kc.abcd(1,2)
	kc.asdf("asdf",1)
	kc=AttachCatcher(k)
