import redis, logging, random
import message_pb2 as msg

class TransientDB:
    rclient=None
    def __init__(self, redis):
        self.rclient=redis
    def getChunkServers(self):
        pass
    def getChunkLocation(self, chunks):
        if isinstance(chunks, msg.Guid):
            # single lookup
            #return ret
            pass
        else:
            # batch lookup
            # return ret
            pass

def guidAssign(x, y):
    x.a=y.a; x.b=y.b; x.c=y.c; x.d=y.d

def isGuidZero(x):
    return (x.a==0 and x.b==0 and x.c==0 and x.d==0)

class MDS:
    tdb=None
    stub=None
    def __init__(self, stub, transientdb):
        self.stub=stub
        self.tdb=transientdb
    def NewChunk(self, arg):
        logging.debug(type(arg))
        if isGuidZero(arg.location):
            servers=self.tdb.getChunkServers()
            x=random.choice(servers).guid
            guidAssign(arg.location, x)
        logging.debug("NewChunk on chunk server %s", arg.location)
        retv=self.stub.callMethod_on("NewChunk", arg, arg.location)
        return retv
    
    @staticmethod    
    def splitLocations(pairs):
        "input should be like [(guid, location), (guid, location), ...], where locations are ordered"
        guids=None; location=None;
        for p in pairs:
            if location==None:
                guids=[p[0],]
                location=p[1]
                continue
            elif location==p[1]:
                guids.append(p[0])
            elif location!=p[1]:
                yield guids, location
                guids=[p[0], ]
                location=p[1]
        yield guids, location

    def DeleteChunk(self, arg):
        logging.debug(type(arg))
        locations=self.tdb.getChunkLocation(arg.guids)
        pairs=[ (arg.guids[i], locations[i]) for i in range(len(arg.guids))]
        pairs.sort(key = lambda x : x[1])
        for guids,location in MDS.splitLocations(pairs):
            arg.guids=guids
            self.stub.callMethod_on("DeleteChunk", arg, location)



        ret=msg.DeleteChunk_Response()
        ret.guid.a=arg.guid.a
        ret.guid.b=arg.guid.b
        ret.guid.c=arg.guid.c
        ret.guid.d=arg.guid.d
        return ret
    def NewVolume(self, arg):
        logging.debug(type(arg))
        ret=msg.NewVolume_Response()
        return ret
    def AssembleVolume(self, arg):
        logging.debug(type(arg))
        ret=msg.AssembleVolume_Response()
        ret.access_point='no access_point'
        return ret
    def DisassembleVolume(self, arg):
        logging.debug(type(arg))
        ret=msg.DisassembleVolume_Response()
        ret.access_point=arg.access_point
        return ret
    def RepairVolume(self, arg):
        logging.debug(type(arg))
        ret=msg.RepairVolume_Response()
        return ret
    def ReadVolume(self, arg):
        logging.debug(type(arg))
        ret=msg.ReadVolume_Response()
        ret.volume.size=0
        return ret
    def WriteVolume(self, arg):
        logging.debug(type(arg))
        ret=msg.WriteVolume_Response()
        return ret
    def MoveVolume(self, arg):
        logging.debug(type(arg))
        ret=msg.MoveVolume_Response()
        return ret
    def ChMod(self, arg):
        logging.debug(type(arg))
        ret=msg.ChMod_Response()
        return ret
    def DeleteVolume(self, arg):
        logging.debug(type(arg))
        ret=msg.DeleteVolume_Response()
        return ret
    def CreateLink(self, arg):
        logging.debug(type(arg))
        ret=msg.CreateLink_Response()
        return ret

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



