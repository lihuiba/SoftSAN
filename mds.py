import redis
import message_pb2 as proto

class MDS:
    def NewChunk(self, arg):
        print type(arg)
        ret=msg.NewChunk_Response()
        ret.size=arg.size
        ret.guid.a=0
        ret.guid.b=1
        ret.guid.c=2
        ret.guid.d=3
        return ret
    def DeleteChunk(self, arg):
        print type(arg)
        ret=msg.DeleteChunk_Response()
        ret.guid.a=arg.guid.a
        ret.guid.b=arg.guid.b
        ret.guid.c=arg.guid.c
        ret.guid.d=arg.guid.d
        return ret
    def NewVolume(self, arg):
        print type(arg)
        ret=msg.NewVolume_Response()
        return ret
    def AssembleVolume(self, arg):
        print type(arg)
        ret=msg.AssembleVolume_Response()
        ret.access_point='no access_point'
        return ret
    def DisassembleVolume(self, arg):
        print type(arg)
        ret=msg.DisassembleVolume_Response()
        ret.access_point=arg.access_point
        return ret
    def RepairVolume(self, arg):
        print type(arg)
        ret=msg.RepairVolume_Response()
        return ret
    def ReadVolume(self, arg):
        print type(arg)
        ret=msg.ReadVolume_Response()
        ret.volume.size=0
        return ret
    def WriteVolume(self, arg):
        print type(arg)
        ret=msg.WriteVolume_Response()
        return ret
    def MoveVolume(self, arg):
        print type(arg)
        ret=msg.MoveVolume_Response()
        return ret
    def ChMod(self, arg):
        print type(arg)
        ret=msg.ChMod_Response()
        return ret
    def DeleteVolume(self, arg):
        print type(arg)
        ret=msg.DeleteVolume_Response()
        return ret
    def CreateLink(self, arg):
        print type(arg)
        ret=msg.CreateLink_Response()
        return ret


CHANNEL_HEARTBEAT='SoftSAN.MDS0.heartbeat'

def server():
	r=redis.client.Redis()
	r.subscribe(CHANNEL)
	while True:
		msg=r.listen()
		if msg.type=='subscribe': 
			continue
		elif msg.type=='unsubscribe':
			break
		elif msg.type!='message' or msg.channel!=CHANNEL
			assert False, "should receive messages from channel '{0}'".format(CHANNEL)
			continue
		msg=proto.



