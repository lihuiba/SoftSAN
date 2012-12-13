import filesystem, stat

class PersistentDB:
	'''PersistentDB is used only for terminology conversion'''
	def __init__(self, rootPath):
		# self.rootpath=rootPath
		# if not self.rootpath.endswith('/'):
		# 	self.rootpath=self.rootpath+'/'
		self.fs=filesystem.FileSystemNaked(rootPath)
	def write(self, path, volume):
		return self.fs.put(path, volume)
	def read(self, path):
		return self.fs.get(path)
	def move(self, src, dest):
		return self.fs.mv(src, dest)
	def delete(self, path):
		return self.fs.rm(path)
	def createDirectory(self, path, parents):
		return self.fs.mkdir(path, parents)
	def deleteDirectory(self, path, fr):
		return self.fs.rmdir(path, fr)
	def listDirectory(self, path):
		ret=[]
		directory=path+'/'
		for name in self.fs.listdir(path):
			item={'name':name}
			fstat=self.fs.stat(directory+name)
			item['mode']=fstat.st_mode
			if stat.S_ISDIR(fstat.st_mode):
				item['size']=0
			else:
				item['size']=fstat.st_size		#todo: real size of the volume
			item['uid']=fstat.st_uid
			item['gid']=fstat.st_gid
			ret.append(item)
		return ret


