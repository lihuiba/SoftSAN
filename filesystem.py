import os, os.path, shutil
import time, datetime, zipfile

def overrides(interface_class):
    def overrider(method):
        assert(method.__name__ in dir(interface_class))
        return method
    return overrider

class FileSystem:
    """an abstract base class that is to be derived to class FilesystemCDP 
    and class HistoryView"""
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
    def exists(self, path):
        """to check whether a file or directory exists"""
        raise NotImplementedError( "Should have implemented this" )
    def mkdir(self, path, p):
        """mkdir, optionally recursively (if p)"""
        raise NotImplementedError( "Should have implemented this" )
    def rmdir(self, path, f, r):
        """remove a directory, optionally forcing remove a non-empty dir (if f), and recursively (if r)"""
        raise NotImplementedError( "Should have implemented this" )
    def mv(self, src, dest):
        """move a file/dir to another place"""
        raise NotImplementedError( "Should have implemented this" )

class FileSystemCDP(FileSystem):
    """a concrete FileSystem class that implements a continueous data protection (CDP).
    Any modification performed by this class can be un-done"""
    root='/'
    def __init__(self, root):
        if not os.path.isdir(root):
            raise IOError("path doesn't exist!")
        self.root=root+'/'

    def check_path(self, path):
        if not path.startswith('/'):
            raise ValueError("relative path is not allowed!")
        p=path
        while len(p)>1:
            p,n=os.path.split(p)
            if n.startswith('.') or n.startswith('__'):
                raise ValueError("file/directory names can NOT start with '.' or '__'.")

    @staticmethod
    def protect_file(path):
        if not os.path.isfile(path):
            raise IOError("file doesn't exist!")
        ppath,fn=os.path.split(path)
        zipfn=ppath+'/.'+fn
        with zipfile.ZipFile(zipfn, 'a') as ar:
            namelist=ar.namelist()
            while True:
                tm=time.time()
                arcfn='{0}.{1:f}'.format(fn, tm)
                if not arcfn in namelist:
                    break
                time.sleep(0)
                print('High change rate on '+path)
            ar.write(path, arcfn)
        return tm

    @staticmethod
    def path_do_split(path):
        a,b=os.path.split(path)
        if b=='':
            a,b=os.path.split(a)
        return a,b

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
            s='\t'.join(log)+'\n'
            fp.write(s)
        return strtime,fn,ppath

    @staticmethod
    def listdir_filtered(apath):
        ldir0=os.listdir(apath)
        ldir=[x for x in ldir0 if not x.startswith('.')]
        return ldir

    @overrides(FileSystem)
    def get(self, path):
        self.check_path(path)
        path=self.root+path
        with open(path, 'rb') as fp:
            data=fp.read()
        return data

    @overrides(FileSystem)
    def listdir(self, path):
        self.check_path(path)
        apath=self.root+path
        ldir=self.listdir_filtered(apath)
        return ldir

    @overrides(FileSystem)
    def put(self, path, data):
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
        self.check_path(path)
        path=self.root+path
        if not os.path.isfile(path):
            raise IOError("target not exists as a file!")
        tms=self.protect_file(path)
        self.protect_dir(path, 'rm', timestamp=tms)
        os.remove(path)

    @overrides(FileSystem)
    def rmdir(self, path, fr=None):
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
        os.rename(apath, newname)
    
    @overrides(FileSystem)
    def exists(self, path):
        self.check_path(path)
        path=self.root+path
        ret=os.path.exists(path)
        return ret
    
    @overrides(FileSystem)
    def mkdir(self, path, r=None):
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
            os.mkdir(apath)
    
    @overrides(FileSystem)
    def mv(self, src, dest):
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

if __name__=="__main__":
    test='d:\\test'
    if os.path.isdir(test):
        shutil.rmtree(test)
    time.sleep(0.5)
    os.makedirs(test)
    test_=test.replace('\\','/')
    cdp=FileSystemCDP(test_)
    cdp.put('/asdf', 'kkkkkkkkkkk')
    cdp.put('/asdf', 'llllll')
    cdp.put('/asdf', 'ooop')
    cdp.put('/adf', 'ooddddp')
    cdp.put('/adf', 'ooop')
    cdp.put('/aaa', 'xxxxxxxxxxxxxxxxxxxxxxx')
    print cdp.get('/asdf')
    print cdp.get('/adf')
    cdp.mkdir('/adf2')
    cdp.mkdir('/adf3/kkk/ccc', r=True)
    try:
        cdp.put('/adf2', 'ooop')
    except IOError as e:
        print e
    cdp.mv('/aaa', '/adf3')
    cdp.mv('/adf', '/asdf')
    cdp.rm('/asdf')
    #cdp.rm('/adf')
    try:
        cdp.rm('/adf')
    except IOError as e:
        print e
    try:
        cdp.rmdir('/adf3')
    except IOError as e:
        print e
    cdp.rmdir('/adf3', fr=True)

    

