import redis, logging, random
import message_pb2 as msg

def guidAssign(x, y):
    x.a=y.a; x.b=y.b; x.c=y.c; x.d=y.d

def isGuidZero(x):
    return (x.a==0 and x.b==0 and x.c==0 and x.d==0)

def guid2Str(x):
    return "%8x-%8x-%8x-%8x" % (x.a, x.b, x.c, x.d)

def guidFromStr(s):
    ret=msg.Guid()
    s=s.split('-')
    ret.a=int(s[0], 16)
    ret.b=int(s[1], 16)
    ret.c=int(s[2], 16)
    ret.d=int(s[3], 16)
    return ret

class TransientDB:
    rclient=None
    def __init__(self, redis):
        self.rclient=redis
    @staticmethod
    def message2dict(message):
        "receive a PB message, returns its guid and a dict describing the message"
        fields=message.ListFields()
        rst={}
        for f in fields:
            name=f[0].name
            value=f[1]
            if isinstance(value, msg.Guid):
                value=guid2Str(value)
            else:
                listable=getattr(value, 'ListFields', None)
                if listable:
                    value=message2dict(value, '')
                else:
                    container=getattr(value, '_values', None)
                    if container:
                        value=[message2dict(x) for x in container]
            rst[name]=value
        return rst
    def putChunkServer(self, cksinfo):
        cksinfo=message2dict(cksinfo)
        cksguid=cksinfo['guid']; 
        del cksinfo['guid'];
        if 'machine' in cksinfo:
            self.putMachine(cksguid, cksinfo['machine'])
            del cksinfo['machine']
        if 'load' in cksinfo:
            self.putLoad(cksguid, cksinfo['load'])
            del cksinfo['load']
        if 'disks'in cksinfo:
            del cksinfo['disks']
        chunkids=[x.cksguid for x in cksinfo.chunks]
        #chunks=[{'size':x.size, 'server':cksguid, 'disk':x.reside_on_disk} for x in cksinfo.chunks]
        cksinfo.chunks=chunkids

        self.rclient.sadd('ChunkServers', cksguid)
        self.rclient.hmset('ChunkServer.'+cksguid, cksinfo)
        for c in cksinfo.chunks:
            chunkid=c.guid
            del c.guid
            c['server']=cksguid
            self.rclient.hmset('Chunk.'+chunkid, c)


    def getChunkServers(self):
        return self.rclient.smembers('ChunkServers')

    def getChunkLocation(self, chunks):
        if isinstance(chunks, msg.Guid):
            # single lookup
            #return ret
            pass
        else:
            # batch lookup
            # return ret
            pass

class MDS:
    tdb=None
    stub=None
    def __init__(self, stub, transientdb):
        self.stub=stub
        self.tdb=transientdb
    def ChunkServerInfo(self, arg):
        pass;
    def NewChunk(self, arg):
        logging.debug(type(arg))
        if isGuidZero(arg.location):
            servers=self.tdb.getChunkServers()
            x=random.choice(servers)
            x=guidFromStr(x)
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
        done=[]; ret=None;
        for guids,location in MDS.splitLocations(pairs):
            arg.guids=guids
            ret=self.stub.callMethod_on("DeleteChunk", arg, location)
            done=done+ret.guids
            if ret.error:
                break;
        ret.guids=done
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



