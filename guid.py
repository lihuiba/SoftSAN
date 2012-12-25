import random
import messages_pb2 as msg

def assign(x, y):
    x.a=y.a; x.b=y.b; x.c=y.c; x.d=y.d

def isZero(x):
    return (x.a==0 and x.b==0 and x.c==0 and x.d==0)

def setZero(x):
    x.a=0; x.b=0; x.c=0; x.d=0

def toStr(x):
    return "%08x-%08x-%08x-%08x" % (x.a, x.b, x.c, x.d)

def toTuple(x):
    return (x.a, x.b, x.c, x.d)

def fromTuple(x):
    ret=msg.Guid()
    ret.a=x[0]
    ret.b=x[1]
    ret.c=x[2]
    ret.d=x[3]
    return ret

def generate(guid=None):
    ret=guid or msg.Guid()
    ret.a=random.randint(0, 0xffffffff)
    ret.b=random.randint(0, 0xffffffff)
    ret.c=random.randint(0, 0xffffffff)
    ret.d=random.randint(0, 0xffffffff)
    return ret

def fromStr(s):
    ret=msg.Guid()
    s=s.split('-')
    ret.a=int(s[0], 16)
    ret.b=int(s[1], 16)
    ret.c=int(s[2], 16)
    ret.d=int(s[3], 16)
    return ret

