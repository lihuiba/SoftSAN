import inspect
import logging
import sys


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

def GetWrapper(obj, f):
	def wrapper(*args, **kwargs):
		try:
			ret=f(obj, *args, **kwargs)
			return ret
		except:
			(E, e, trace) = sys.exc_info()
			logging.error('exception caught: '+str(e))
			logging.error(str(trace))
	return wrapper


def AttachCatcher(obj, functions):
	cdict=obj.__class__.__dict__
	if functions==None:
		functions=cdict
	if isinstance(functions, basestring):
		functions=(functions,)
	for func in functions:
		f=cdict[func]
		if not inspect.isfunction(f):
			continue
		wrapper=GetWrapper(obj, f)
		obj.__dict__[func]=wrapper
	return obj

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
	kc.abcd(2)
	# kc.abcd(2)
	kc.asdf("asdf")
	print ("OK")
