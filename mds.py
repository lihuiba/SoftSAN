import logging, random
import messages_pb2 as msg
import guid as Guid
import transientdb
import persistentdb

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

def object2message(object, message):
    if isinstance(object, dict):
        d=object
    else:
        d=object.__dict__
    for key in d.keys():
        if key.startswith('_'):
            continue
        setattr(message, key, d[key])

class MDS:
    def __init__(self, transientdb, persistentdb):
        self.service=None
        self.tdb=transientdb
        self.pdb=persistentdb
    def ChunkServerInfo(self, arg):
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
        ret=msg.ReadVolume_Response()
        ret.volume=self.pdb.read(arg.fullpath)
        return ret
    def WriteVolume(self, arg):
        ret=msg.WriteVolume_Response()
        self.pdb.write(arg.fullpath, arg.volume)
        return ret
    def MoveVolume(self, arg):
        ret=msg.MoveVolume_Response()
        self.pdb.move(arg.source, arg.destination)
        return ret
    # def ChMod(self, arg):
    #     ret=msg.ChMod_Response()
    #     return ret
    def DeleteVolume(self, arg):
        ret=msg.DeleteVolume_Response()
        self.pdb.delete(arg.fullpath)
        return ret
    def CreateDirectory(self, arg):
        ret=msg.CreateDirectory_Response()
        self.pdb.createDirectory(arg.fullpath, arg.parents)
        return ret
    def DeleteDirectory(self, arg):
        ret=msg.DeleteDirectory_Response()
        self.pdb.deleteDirectory(arg.fullpath, arg.recursively_forced)
        return ret
    def ListDirectory(self, arg):
        ret=msg.ListDirectory_Response()
        listdir=self.pdb.listDirectory(arg.fullpath)
        for item in listdir:
            it=ret.items.add()
            object2message(item, it)
        return ret


    # def CreateLink(self, arg):
    #     ret=msg.CreateLink_Response()
    #     return ret

#############################################################################

def buildMDS():
    import filesystem
    testdir=filesystem.test_prepare_dir('/mds')
    pdb=persistentdb.PersistentDB(testdir)
    tdb=transientdb.TransientDB()
    server=MDS(tdb, pdb)
    return server    

def test_main():
    import gevent.server, rpc
    server=buildMDS()
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

def test():
    server=buildMDS()
    arg=Object()
    arg.fullpath='/asdf'
    arg.volume='hahahahha'
    ret=server.WriteVolume(arg)
    print ret
    ret=server.ReadVolume(arg)
    print ret
    arg=Object()
    arg.fullpath='/nnd/kkk/mdk'
    arg.parents=True
    ret=server.CreateDirectory(arg)
    print ret
    arg.fullpath='/mds'
    ret=server.CreateDirectory(arg)
    print ret
    arg=Object()
    arg.fullpath='/'
    ret=server.ListDirectory(arg)
    print ret
    arg.fullpath='/nnd'
    arg.recursively_forced=True
    ret=server.DeleteDirectory(arg)
    print ret



if __name__=="__main__":
    logging.basicConfig(level=logging.DEBUG)
    test_main()
    #test_split()
    #test()
