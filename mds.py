import redis, logging, random
import messages_pb2 as msg
import guid as Guid
import rpc, transientdb
import gevent.server

class Object:
    def __init__(self, d=None):
        if isinstance(d, dict):
            self.__dict__=d

def splitbyattr(objs, key):
    objs.sort(key = lambda x : getattr(x, key))
    group=None
    attr=None
    for p in objs:
        value=getattr(p, key)
        if attr==None:
            group=[p, ]
            attr=value
            continue
        elif attr==value:
            group.append(p)
        elif attr!=value:
            yield group
            group=[p, ]
            attr=value
    if group!=None:
        yield group

def message2object(message):
    "receive a PB message, returns its guid and a object describing the message"
    fields=message.ListFields()
    rst=Object()
    for f in fields:
        name=f[0].name
        value=f[1]
        if isinstance(value, msg.Guid):
            value=Guid.toStr(value)
        else:
            listable=getattr(value, 'ListFields', None)
            if listable:
                value=message2object(value, '')
            else:
                container=getattr(value, '_values', None)
                if container:
                    value=[message2object(x) for x in container]
        setattr(rst, name, value)
    return rst


class MDS:
    def __init__(self, transientdb):
        self.service=None
        self.tdb=transientdb
    def ChunkServerInfo(self, arg):
        logging.debug(type(arg))
        cksinfo=message2object(arg)
        cksinfo.guid=Guid.toStr(self.service.peerGuid())
        self.tdb.putChunkServer(cksinfo)
        print cksinfo.__dict__
    def GetChunkServers(self, arg):
        ret=msg.GetChunkServers_Response()
        servers=self.tdb.getChunkServerList()
        servers=self.tdb.getChunkServers(servers)
        for s in servers:
            print s.__dict__
            ret.random.add()
            t=ret.random[-1]
            t.ServiceAddress=s.ServiceAddress
            t.ServicePort=int(s.ServicePort)
        return ret
    # def NewChunk(self, arg):
    #     logging.debug(type(arg))
    #     if isGuidZero(arg.location):
    #         servers=self.tdb.getChunkServerList()
    #         x=random.choice(servers)
    #         x=guidFromStr(x)
    #         guidAssign(arg.location, x)
    #     logging.debug("NewChunk on chunk server %s", arg.location)
    #     retv=self.stub.callMethod_on("NewChunk", arg, arg.location)
    #     return retv    
    # def DeleteChunk(self, arg):
    #     logging.debug(type(arg))
    #     chunks=self.tdb.getChunks(arg.guids)
    #     done=[]
    #     for cgroup in splitbyattr(chunks, 'serverid'):
    #         arg.guids.clear()
    #         for c in cgroup:
    #             guid=guidFromStr(c.guid)
    #             arg.guids.add()
    #             guidAssign(arg.guids[-1], guid)
    #         ret=self.stub.callMethod_on("DeleteChunk", arg, cgroup[0].serverid)
    #         done=done+ret.guids
    #         if ret.error:
    #             break;
    #     ret.guids=done
    #     return ret
    # def AssembleVolume(self, arg):
    #     logging.debug(type(arg))
    #     ret=msg.AssembleVolume_Response()
    #     ret.access_point='no access_point'
    #     return ret
    # def DisassembleVolume(self, arg):
    #     logging.debug(type(arg))
    #     ret=msg.DisassembleVolume_Response()
    #     ret.access_point=arg.access_point
    #     return ret
    # def RepairVolume(self, arg):
    #     logging.debug(type(arg))
    #     ret=msg.RepairVolume_Response()
    #     return ret
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
    # def ChMod(self, arg):
    #     logging.debug(type(arg))
    #     ret=msg.ChMod_Response()
    #     return ret
    def DeleteVolume(self, arg):
        logging.debug(type(arg))
        ret=msg.DeleteVolume_Response()
        return ret
    # def CreateLink(self, arg):
    #     logging.debug(type(arg))
    #     ret=msg.CreateLink_Response()
    #     return ret


def test_main():
    tdb=transientdb.TransientDB()
    server=MDS(tdb)
    service=rpc.RpcService(server)
    framework=gevent.server.StreamServer(('0.0.0.0', 2345), service.handler)
    framework.serve_forever()


def test_split():
    o1={ 'asdf':123, 'des':823}
    o2={ 'asdf':231, 'des':238}
    o3={ 'asdf':312, 'des':382}
    lst=[Object(o1), Object(o2), Object(o3)]    
    for group in splitbyattr(lst, 'des'):
        for c in group:
            print c.__dict__

if __name__=="__main__":
    logging.basicConfig(level=logging.DEBUG)
    test_main()
    #test_split()
