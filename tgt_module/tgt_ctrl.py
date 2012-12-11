#/usr/sbin/
from objects_tgt import *
from process_call import *
import re


def buildArguments(mode, op, **kwargs):
	ret=["tgtadm", "--lld", "iscsi", "--mode", mode, "--op", op]
	for key in kwargs:
		if len(key)>1:
			skey='--'+key
		else:
			skey='-'+key
		ret.append(skey)
		ret.append(str(kwargs[key]))
	return ret

class Tgt:
	'''
        attribute: targetlist
    '''
	def __init__(self, targetlist=None):
		self.targetlist = targetlist or []
		self.table1=[
			(re.compile(r'^Target (\d*): (.*)$'),			self.newTarget),
			(re.compile(r'^LUN: (\d*)$'), 					self.newLun),
			(re.compile(r'^Size: (.*), Block size: (.*)$'),	self.setSize),
		]
		self.table2=['SCSI ID', 'SCSI SN', 'Online', 'Removable media', 'Prevent removal', \
		'Readonly', 'Backing store type', 'Backing store path', 'Backing store flags']

	def parseLines(self, lines):
		for line in lines:
			line=line.strip()
			for pattern in self.table1:
				match=pattern[0].match(line)
				if match:
					g=match.groups()
					pattern[1](g)
			try:
				(key,value)=line.split(': ')
			except:
				continue
			if key in self.table2:
				lun=self.targetlist[-1].lunlist[-1]
				setattr(lun, key, value)

	def newTarget(self, group):
		id=group[0]
		iqn=group[1]
		t=Target(id, iqn)
		self.targetlist.append(t)
	def newLun(self, group):
		id=group[0]
		lun=Lun(id)
		self.targetlist[-1].lunlist.append(lun)
	def setSize(self, group):
		lunsize=group[0]
		blocksize=group[1]
		lun=self.targetlist[-1].lunlist[-1]
		lun.size=lunsize
		lun.blocksize=blocksize

	def reload(self):
	    argv = list()
	    argv.append("tgtadm")
	    argv.append("--lld")
	    argv.append("iscsi")
	    argv.append("--mode")
	    argv.append("target")
	    argv.append("--op")
	    argv.append("show")

	    argv=buildArguments(mode='target', op='show')

	    # list target and lun
	    (status, output) = process_call_argv(argv)
	    if status != 0:
	        print "error occur reload target"
	        return output
	    # record target and lun
	    self.targetlist = list()
	    n_tgt = 0
	    n_lun = 0
	    # loop = 0
	    for line in output.splitlines():##############################splitlines
	        # loop = loop+1
	        if line.find('Target') != -1:
	            n_tgt = n_tgt + 1
	            n_lun = 0
	            strlist = line.split(':',1)
	            target_id = filter(str.isdigit,strlist[0])
	            target_name = strlist[1]
	            self.targetlist.append(Target(target_id, target_name))

	        elif line.find('LUN:') != -1:
	            n_lun = n_lun + 1
	            strlist = line.split(':')
	            lun_index = strlist[1].strip()
	            self.targetlist[n_tgt-1].lunlist.append(Lun(lun_index))

	        elif line.find('Size:') != -1:
	            strlist = line.split(',')
	            lun_size = strlist[0].split(':')[1].strip()
	            blocksize = strlist[1].split(':')[1].strip()
	            self.targetlist[n_tgt-1].lunlist[n_lun-1].size = lun_size
	            self.targetlist[n_tgt-1].lunlist[n_lun-1].blocksize = blocksize

	        elif line.find('Backing store path:') != -1:
	            strlist = line.split(':')
	            path = strlist[1].strip()
	            self.targetlist[n_tgt-1].lunlist[n_lun-1].backing_store_path = path
	        
	def print_out(self):
	    for target in self.targetlist:
	        print '|'+('target name:'+target.name).center(90,'-')+'|'
	        # print '|'+('number of lun:'+str(len(target.lunlist)).ljust(16,'-')+'|'
	        for lun in target.lunlist:
	            out = 'lunindex:'+lun.index+','+'blocksize:'+lun.blocksize+','+'size:'+lun.size+','+'path:'+lun.backing_store_path
	            print '|'+out.ljust(90)+'|'
	    print '|'+''.ljust(90,'-')+'|'

	def path2target_id(self, path):
		for target in self.targetlist:
			for lun in target.lunlist:
				if cmp(lun.backing_store_path, path)==0:
					target_id = target.id
					return target_id
		return None
	    
	def new_target(self, target_id, target_name):
	    argv = list()
	    argv.append('tgtadm')
	    argv.append('--lld')
	    argv.append('iscsi')
	    argv.append('--mode')
	    argv.append('target')
	    argv.append('--op')
	    argv.append('new')
	    argv.append('--tid')
	    argv.append(target_id)
	    argv.append('-T')
	    argv.append(target_name)
	    # execute command
	    (status, output) = process_call_argv(argv)
	    if status != 0:
	        print "error occur during adding target"
	        return output
	    print output
	    return None

	def bind_target(self, target_id, acl):
	    argv = list()
	    argv.append('tgtadm')
	    argv.append('--lld')
	    argv.append('iscsi')
	    argv.append('--mode')
	    argv.append('target')
	    argv.append('--op')
	    argv.append('bind')
	    argv.append('--tid')
	    argv.append(target_id)
	    argv.append('-I')
	    argv.append(acl)
	    # execute command
	    (status, output) = process_call_argv(argv)
	    if status != 0:
	        print "error occur during binding target"
	        return output
	    print output
	    return None

	def delete_target(self, target_id):
	    argv = list()
	    argv.append('tgtadm')
	    argv.append('--lld')
	    argv.append('iscsi')
	    argv.append('--mode')
	    argv.append('target')
	    argv.append('--op')
	    argv.append('delete')
	    argv.append('--tid')
	    argv.append(target_id)
	    # execute command
	    (status, output) = process_call_argv(argv)
	    if status != 0:
	        print "error occur during deleting target"
	        return output
	    print output
	    return None

	def new_lun(self, tid, lun_index, path):
	    argv = list()
	    argv.append('tgtadm')
	    argv.append('--lld')
	    argv.append('iscsi')
	    argv.append('--mode')
	    argv.append('logicalunit')
	    argv.append('--op')
	    argv.append('new')
	    argv.append('--tid')
	    argv.append(tid)
	    argv.append('--lun')
	    argv.append(lun_index)
	    argv.append('-b')
	    argv.append(path)
	    # execute command
	    (status, output) = process_call_argv(argv)
	    if status != 0:
	        print "error occur during adding lun"
	        return output
	    print output
	    return None

	def delete_lun(self, tid, lun_index):
	    argv = list()
	    argv.append('tgtadm')
	    argv.append('--lld')
	    argv.append('iscsi')
	    argv.append('--mode')
	    argv.append('logicalunit')
	    argv.append('--op')
	    argv.append('delete')
	    argv.append('--tid')
	    argv.append(tid)
	    argv.append('--lun')
	    argv.append(lun_index)
	    # execute command
	    (status, output) = process_call_argv(argv)
	    if status != 0:
	        print "error occur during deleting lun"
	        return output
	    print output
	    return None



if __name__=="__main__":
	lines=open('t.txt').readlines()
	t=Tgt()
	t.parseLines(lines)
	for target in t.targetlist:
		print target.__dict__
		for lun in target.lunlist:
			print '\t', lun
			for key in lun.__dict__:
				print '\t\t',key,': ',lun.__dict__[key]

