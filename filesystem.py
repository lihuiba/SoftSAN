import os, os.path, shutil, logging
import time, datetime, zipfile
import catcher

def overrides(interface_class):
    def overrider(method):
        assert(method.__name__ in dir(interface_class))
        return method
    return overrider

#TODO: GC, restore and history view
class FileSystem:
    """an abstract base class that is to be derived to class FilesystemCDP 
    and class HistoryView"""
    root='/'
    def __init__(self, root):
        if not os.path.isdir(root):
            raise IOError("path doesn't exist!")
        self.root=root+'/'
    def get(self, path):
        """read thet content of a file"""
        raise NotImplementedError( "Should have implemented this" )
    def put(self, path, data):
        """update the content of a file, potentionally created it"""
        raise NotImplementedError( "Should have implemented this" )
    def listdir(self, path):
        """list the content of a directory"""
        raise NotImplementedError( "Should have implemented this" )
    def rm(self, path):
        """remove a file"""
        raise NotImplementedError( "Should have implemented this" )
    def stat(self, path):
        """stat"""
        raise NotImplementedError( "Should have implemented this" )
    def exists(self, path):
        """to check whether a file or directory exists"""
        raise NotImplementedError( "Should have implemented this" )
    def mkdir(self, path, p):
        """mkdir, optionally recursively (if p)"""
        raise NotImplementedError( "Should have implemented this" )
    def rmdir(self, path, fr):
        """remove a directory, optionally recursively forcing remove a non-empty dir (if fr)"""
        raise NotImplementedError( "Should have implemented this" )
    def mv(self, src, dest):
        """move a file/dir to another place"""
        raise NotImplementedError( "Should have implemented this" )
    @staticmethod
    def check_path(path):
        if not path.startswith('/'):
            raise ValueError("relative path is not allowed!")
        p=path
        while True:
            p,n=os.path.split(p)
            if n=='': break
            if n.startswith('.') or n.startswith('__'):
                raise ValueError("file/directory names can NOT start with '.' or '__'.")
    @staticmethod
    def path_do_split(path):
        a,b=os.path.split(path)
        if b=='':
            a,b=os.path.split(a)
        return a,b

class FileSystemNaked(FileSystem):
    """an naked filesystem implemention, which passes requests directly to OS"""
    @overrides(FileSystem)
    def get(self, path):
        logging.debug("@get: "+path)
        self.check_path(path)
        apath=self.root+path
        with open(apath, 'rb') as fp:
            data=fp.read()
        return data

    @overrides(FileSystem)
    def listdir(self, path):
        logging.debug("@listdir: "+path)
        self.check_path(path)
        apath=self.root+path
        ldir=os.listdir(apath)
        return ldir

    @overrides(FileSystem)
    def put(self, path, data):
        logging.debug("@put: "+path)
        self.check_path(path)
        apath=self.root+path
        with open(apath, 'wb') as fp:
            fp.write(data)

    @overrides(FileSystem)
    def rm(self, path):
        logging.debug("@rm: "+path)
        self.check_path(path)
        apath=self.root+path
        os.remove(apath)

    @overrides(FileSystem)
    def rmdir(self, path, fr=None):
        logging.debug("@rmdir (-fr=={0}) '{1}'".format(fr,path))
        self.check_path(path)
        apath=self.root+path
        if not os.path.isdir(apath):
            raise IOError("target not exists as a directory!")
        if fr:
            shutil.rmtree(apath)
        else:
            os.rmdir(apath)
    
    @overrides(FileSystem)
    def exists(self, path):
        logging.debug("@exists: "+path)
        self.check_path(path)
        apath=self.root+path
        ret=os.path.exists(apath)
        return ret

    @overrides(FileSystem)
    def stat(self, path):
        logging.debug("@stat: "+path)
        self.check_path(path)
        apath=self.root+path
        ret=os.stat(apath)
        return ret
    
    @overrides(FileSystem)
    def mkdir(self, path, p=None):
        logging.debug("@mkdir (-p=={0}): '{1}'".format(p, path))
        self.check_path(path)
        apath=self.root+path               #absolute path
        if p:
            os.makedirs(apath)
        else:
            os.mkdir(apath)
    
    @overrides(FileSystem)
    def mv(self, src, dest):
        logging.debug("@mv from '{0}' to {1}".format(src, dest))
        self.check_path(src)
        self.check_path(dest)
        asrc=self.root+src
        adest=self.root+dest
        shutil.move(asrc, adest)

class Archive(zipfile.ZipFile):
    names=None
    timestamps=None
    def __enter__(self):
        return self
    def __exit__(self, exc_type, exc_value, traceback):
        self.close()
        return True
    
    @staticmethod
    def name2TimeStamp(name):
        parts=name.rsplit('.', 2)
        if len(parts)!=3 or not parts[1].isdigit() or not parts[2].isdigit():
            msg="unrecognized file name {0} in archive {1}".format(name, afn)
            logging.warning(msg)
            return 
        ts=float(parts[1]+'.'+parts[2])
        return ts

    def getTimeStamps(self):
        if self.timestamps!=None: 
            return self.timestamps
        self.names=self.namelist()
        self.timestamps=[]
        for name in self.names:
            ts=name2TimeStamp(name)
            self.timestamps.append( (name,ts) )
        self.timestamps.sort(key=(lambda x:x[1]), reverse=True)
        return self.timestamps

    def isArchiveOlder(self, threshold):
        tss=self.getTimeStamps()
        return tss[-1][1]<threshold

    def lowerBound(self, threshold):
        """this function returns the smallest index of 
        self.timestamps whose [1] is smaller than threshold,
        returns None if it doesn't exist."""
        for i in range(len(self.timestamps)):
            if self.timestamps[i][1]<threshold:
                return i
        return None

    def upperBound(self, threshold):
        """this function returns the biggest index of 
        self.timestamps whose [1] is bigger than threshold,
        returns None if it doesn't exist."""
        if self.timestamps[0][1]<threshold:
            return None
        for i in range(len(timestamps)):
            if self.timestamps[i][1]>threshold:
                return i-1
        return None

class FileSystemCDP(FileSystem):
    """a concrete FileSystem class that implements a continueous data protection (CDP).
    Any modification performed by this class can be un-done"""
    @staticmethod
    def protect_file(path):
        if not os.path.isfile(path):
            raise IOError("file doesn't exist!")
        ppath,fn=os.path.split(path)
        zipfn=ppath+'/.'+fn
        with Archive(zipfn, 'a') as ar:
            namelist=ar.namelist()
            while True:
                tm=time.time()
                arcfn='{0}.{1:f}'.format(fn, tm)
                if not arcfn in namelist:
                    break
                time.sleep(0)
                print('High change rate on '+path)
            ar.write(path, arcfn)
        logging.debug('protected file: '+path)
        return tm

    @staticmethod
    def protect_dir(path, op, arg=None, timestamp=None):
        ppath,fn=FileSystemCDP.path_do_split(path)
        if not os.path.exists(ppath):
            raise IOError("path doesn't exist!")
        if not timestamp:
            strtime=str(datetime.datetime.now())
        else:
            strtime=str(datetime.datetime.fromtimestamp(timestamp))
        if op=='add' or op=='rm':
            log=(strtime, op, fn)
        elif op=='mv':
            ppath_arg,fn_arg=FileSystemCDP.path_do_split(arg)
            if ppath_arg==ppath or ppath_arg=='':
                log=(strtime, op, fn, fn_arg)
            else:
                log=(strtime, op, fn, arg)
        else:
            raise ValueError("op should be add, rm or mv")
        difn=ppath+'/.__dirinfo__'
        with open(difn, 'a') as fp:
            s='\t'.join(log)
            fp.write(s+'\n')
        logging.debug("protected dir: '{0}' with journal '{1}'".format(path, s))
        return strtime,fn,ppath

    @staticmethod
    def listdir_filtered(apath):
        ldir0=os.listdir(apath)
        ldir=[x for x in ldir0 if not x.startswith('.')]
        return ldir

    @overrides(FileSystem)
    def get(self, path):
        logging.debug("@get: "+path)
        self.check_path(path)
        path=self.root+path
        with open(path, 'rb') as fp:
            data=fp.read()
        return data

    @overrides(FileSystem)
    def listdir(self, path):
        logging.debug("@listdir: "+path)
        self.check_path(path)
        apath=self.root+path
        ldir=self.listdir_filtered(apath)
        return ldir

    @overrides(FileSystem)
    def put(self, path, data):
        logging.debug("@put: "+path)
        self.check_path(path)
        path=self.root+path
        if os.path.isdir(path):
            raise IOError("target already exists as a directory!")
        if not os.path.exists(path):
            self.protect_dir(path, 'add')
        else:
            self.protect_file(path)
        with open(path, 'wb') as fp:
            fp.write(data)

    @overrides(FileSystem)
    def rm(self, path):
        logging.debug("@rm: "+path)
        self.check_path(path)
        path=self.root+path
        if not os.path.isfile(path):
            raise IOError("target not exists as a file!")
        tms=self.protect_file(path)
        self.protect_dir(path, 'rm', timestamp=tms)
        os.remove(path)

    @overrides(FileSystem)
    def rmdir(self, path, fr=None):
        logging.debug("@rmdir (-fr=={0}) '{1}'".format(fr,path))
        self.check_path(path)
        apath=self.root+path
        if not os.path.isdir(apath):
            raise IOError("target not exists as a directory!")
        ldir=self.listdir_filtered(apath)
        if fr:
            for item in ldir:
                item=apath+'/'+item
                dest=item[len(self.root):]
                if os.path.isdir(item):
                    print "self.rmdir -fr "+dest
                    self.rmdir(dest, fr=True)
                else:
                    print "self.rm "+dest
                    self.rm(dest)
        else:
            if len(ldir)>0:
                raise IOError("target directory not empty!")
        _,name,ppath=self.protect_dir(apath, 'rm')
        newname=ppath+'/.'+name
        os.rename(apath, newname)    #todo: combine existing '.xxx' directory
    
    @overrides(FileSystem)
    def exists(self, path):
        logging.debug("@exists: "+path)
        self.check_path(path)
        path=self.root+path
        ret=os.path.exists(path)
        return ret
    
    @overrides(FileSystem)
    def mkdir(self, path, r=None):
        logging.debug("@mkdir (-r=={0}): '{1}'".format(r, path))
        self.check_path(path)
        apath=self.root+path               #absolute path
        if os.path.exists(apath):
            raise IOError("target exists!")
        if r:
            ppath,_=self.path_do_split(path)
            if not os.path.isdir(self.root+ppath):
                self.mkdir(ppath, r=True)
            self.mkdir(path, r=False)
        else:
            ppath,target=self.path_do_split(apath)     #parent path
            if not os.path.isdir(ppath):
                raise IOError("parent directory doesn't exist!")
            self.protect_dir(apath, 'add')
            old_target=ppath+'.'+target
            if os.path.isdir(old_target):
                os.rename(old_target, apath)
            else:
                os.mkdir(apath)
    
    @overrides(FileSystem)
    def mv(self, src, dest):
        logging.debug("@mv from '{0}' to {1}".format(src, dest))
        self.check_path(src)
        asrc=self.root+src
        if not os.path.exists(asrc):
            raise IOError("source target NOT exists!")
        self.check_path(dest)
        adest=self.root+dest
        if os.path.isfile(adest):
            if os.path.isdir(asrc):
                raise IOError("can NOT move a directory into a file!")
            self.rm(dest)
            arg=dest
        elif os.path.isdir(adest):
            _,fn=self.path_do_split(src)
            arg=dest+'/'+fn
        else: #if not os.path.exists(adest):
            dppath,_=self.path_do_split(adest)
            if not os.path.isdir(dppath):
                raise IOError("destination directory NOT exists!")
            arg=dest
        self.protect_dir(asrc, 'mv', arg)
        shutil.move(asrc, adest)

class GCbyExpiration:
    cdp=None
    expiration=30
    def __init__(self, cdp, expiration=30):
        """the cdp object, and expiration (days)"""
        self.expiration=expiration*24*60*60   #converting days into seconds
        self.cdp=cdp

    def gc(self):
        threshold=time.time()-expiration
        for dir,_,files in os.walk(self.cdp.root):
            files=[x for x in files if x.startswith('.') and not x.startswith('..')]
            for afn in files:
                pafn=os.path.join(dir, afn)
                with Archive(afn, 'r') as ar:
                    old=ar.isArchiveOlder(threshold)
                if old:
                    # rename '.xxx'(afn) into '..xxx'(aafn), 
                    # potentially deleting an existing latter one.
                    aafn='.'+afn
                    paafn=os.path.join(dir, aafn)
                    if os.path.isfile(paafn):
                        os.remove(paafn)
                    elif os.path.isdir(paafn):
                        raise IOError("path '{0}' should not be a directory!".format(paafn))
                    os.rename(pafn, paafn); 

class HistoryView(FileSystem):
    cdp=None
    timestamp=None
    def __init__(self, cdp, timestamp=None):
        self.cdp=cdp
        if timestamp==None: self.timestamp=timestamp
        else: self.timestamp=time.time()
    
    def timeGoto(self, timestamp):
        self.timestamp=timestamp

    @staticmethod
    def pathSplitAll(path):
        ret=[]
        head=path
        while True:
            (head, tail) = os.path.split(head)
            if tail=='': break
            ret.append(tail)
        ret.reverse()
        return ret

    def readDirInfo(self, path):
        ret=[]
        fn=os.path.join(self.cdp.root, path, '.__dirinfo__')
        with open(fn) as fp:
            for line in fp:
                sp=line.split('\t',2)
                tm=datetime.datetime.strptime(sp[0], '%Y-%m-%d %H:%M:%S.%f')
                op=sp[1]
                fn=sp[2]
                ret.add( (tm,op,fn) )
        return ret

    @overrides(FileSystem)
    def get(self, path):
        self.check_path(path)
        dirs=pathSplitAll(path)
        filename=dirs.pop()
        current=self.cdp.root
        for item in dirs:
            temp=os.path.join(current, item)
            if os.path.isdir(temp):
                current=temp
                continue
            temp=os.path.join(current, '.'+item)
            if os.path.isdir(temp):
                current=temp
                continue
            raise IOError("incorrect path: '{0}'.".format(path))


    # HistoryView doesn't support modification
    # @overrides(FileSystem)
    # def put(self, path, data):
    #     pass

    @overrides(FileSystem)
    def listdir(self, path):
        pass

    # HistoryView doesn't support modification
    # @overrides(FileSystem)
    # def rm(self, path):
    #     pass

    @overrides(FileSystem)
    def exists(self, path):
        pass

    # HistoryView doesn't support modification
    # @overrides(FileSystem)
    # def mkdir(self, path, p):
    #     pass

    # HistoryView doesn't support modification
    # @overrides(FileSystem)
    # def rmdir(self, path, f, r):
    #     pass

    # HistoryView doesn't support modification
    # @overrides(FileSystem)
    # def mv(self, src, dest):
    #     pass

############################# TESTs #############################################
def test_filesystem(fs):
    fs=catcher.AttachCatcher(fs)
    fs.put('/asdf', 'kkkkkkkkkkk')
    fs.put('/asdf', 'llllll')
    fs.put('/asdf', 'ooop')
    fs.put('/adf', 'ooddddp')
    fs.put('/adf', 'ooop')
    fs.put('/aaa', 'xxxxxxxxxxxxxxxxxxxxxxx')
    print fs.get('/asdf')
    print fs.get('/adf')
    fs.mkdir('/adf2')
    fs.mkdir('/adf3/kkk/ccc', r=True)
    fs.put('/adf2', 'ooop')
    fs.mv('/aaa', '/adf3')
    fs.mv('/adf', '/asdf')
    fs.rm('/asdf')
    #fs.rm('/adf')
    fs.rm('/adf')
    fs.rmdir('/adf3')
    fs.rmdir('/adf3', fr=True)

def test_prepare_dir(dir):
    dir=os.path.expanduser('~/santest/'+dir)
    if os.path.isdir(dir):
        shutil.rmtree(dir)
    time.sleep(0.5)
    os.makedirs(dir)
    return dir

def test_filesystemCDP():
    dir=test_prepare_dir('/fscdp/')
    cdp=FileSystemCDP(dir)
    test_filesystem(cdp)

def test_filesystemNaked():
    dir=test_prepare_dir('/fsnaked/')
    naked=FileSystemNaked(dir)
    test_filesystem(naked)

if __name__=="__main__":
    logging.basicConfig(level=logging.DEBUG)
    test_filesystemCDP()
    test_filesystemNaked()
