import redis, logging


def container(x):
    if isinstance(x, str):
        return (x,),True
    if hasattr(x, '__iter__') or hasattr(x, '__getitem__'):
        return x,False
    else:
        return (x,),True

def uncontainer(x, flag):
    if flag:
        return x[0]
    else:
        return x

class Object:
    def __init__(self, d=None):
        if isinstance(d, dict):
            self.__dict__=d

TTL=60 #seconds
class TransientDB:
    rclient=None
    def __init__(self, redisc=None):
        self.rclient = redisc or redis.client.Redis()
    def putObjects(self, keyprefix, objects):
        objects,_=container(objects)
        for o in objects:
            key=keyprefix+o.guid
            if not isinstance(o, dict):
                o=o.__dict__
            self.rclient.hmset(key, o)
            self.rclient.expire(key, TTL)
    def putMachine(self, serverid, machine):
        print serverid, machine
    def putChunkServer(self, cksinfo):
        cksguid=cksinfo.guid; 
        if hasattr(cksinfo, 'machine'):
            self.putMachine(cksguid, cksinfo.machine)
            del cksinfo.machine
        if hasattr(cksinfo, 'load'):
            self.putLoad(cksguid, cksinfo.load)
            del cksinfo.load
        if hasattr(cksinfo, 'disks'):
            del cksinfo.disks            
        chunks=cksinfo.chunks
        cksinfo.chunks=[x.guid for x in chunks]
        self.putObjects('ChunkServer.', cksinfo)
        self.putChunks(cksguid, chunks)
    def putChunks(self, serverid, chunks):
        chunks,_=container(chunks)
        for c in chunks:
            c.server=serverid
        self.putObjects('Chunk.', chunks)
    def getObjects(self, keyprefix, objectids):
        objectids,flag=container(objectids)
        print flag
        objects =[ Object(self.rclient.hgetall(keyprefix+id)) for id in objectids ]
        return uncontainer(objects, flag)
    def getChunkServers(self, serverids):
        return self.getObjects('ChunkServer.', serverids)
    def getChunks(self, chunkids):
        return self.getObjects('Chunk.', chunkids)
    def getList(self, keyprefix):
        plen=len(keyprefix)
        ret0=self.rclient.keys(keyprefix+'*')        
        ret=[ x[plen:] for x in ret0 ]             # extract the guid part of each element
        return ret
    def getChunkServerList(self):
        return self.getList('ChunkServer.')
    def getChunkList(self):
        return self.getList('Chunk.')

if __name__=="__main__":
    tdb=TransientDB()
    d1={
        'guid' : '112233445566',
        'chunks' : []
    }
    d2= {
        'guid' : '6677889900',
        'size' : 64
    }

    info=Object(d1)
    c=Object(d2)
    c.__dict__=d2
    info.chunks.append(c)
    tdb.putChunkServer(info)
    print tdb.getChunkServerList()
    print tdb.getChunkList()
    print tdb.getChunks(c.guid).__dict__
    print tdb.getChunkServers(info.guid).__dict__
