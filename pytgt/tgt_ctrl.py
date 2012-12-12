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
			(re.compile(r'^Target (\d*): (.*)$'),			self.parseTarget),
			(re.compile(r'^LUN: (\d*)$'), 					self.parseLun),
			(re.compile(r'^Size: (.*), Block size: (.*)$'),	self.parseSize),
		]
		self.table2=['Driver', 'State']
		self.table3=['Type', 'SCSI ID', 'SCSI SN', 'Online', 'Removable media', 'Prevent removal', \
					 'Readonly', 'Backing store type', 'Backing store path', 'Backing store flags']

	def parseLines(self, lines):
		for line in lines:
			line=line.strip()
			for pattern in self.table1:
				match=pattern[0].match(line)
				if match:
					g=match.groups()
					pattern[1](*g)
			try:
				(key,value)=line.split(': ', 1)
			except:
				continue
			if key in self.table2:
				target=self.targetlist[-1]
				setattr(target, key, value)
			elif key in self.table3:
				if key=='SCSI ID':
					key = 'scsi_id'
				elif key=='SCSI SN':
					key = 'scsi_sn'
				elif key=='Removable media':
					key = 'Removable_media'
				elif key=='Prevent removal':
					key = 'Prevent_removal'
				elif key=='Backing store type':
					key = 'backing_store_type'
				elif key=='Backing store path':
					key = 'backing_store_path'
				elif key=='Backing store flags':
					key = 'backing_store_flags'
				lun=self.targetlist[-1].lunlist[-1]
				setattr(lun, key, value)

	def parseTarget(self, id, iqn):
		t=Target(id, iqn)
		self.targetlist.append(t)

	def parseLun(self, id):
		lun=Lun(id)
		self.targetlist[-1].lunlist.append(lun)

	def parseSize(self, lunsize, blocksize):
		lun=self.targetlist[-1].lunlist[-1]
		lun.size=lunsize
		lun.blocksize=blocksize

	def reload(self):
	    argv = list()
	    argv=buildArguments(mode='target', op='show')
	    # print argv
	    (status, output) = process_call_argv(argv)
	    # print output
	    self.targetlist = []
	    self.parseLines(output.splitlines())
	    if status != 0:
	        print "error occur reload target"
	    return output
			        
	def print_out(self):
	    for target in self.targetlist:
	        print '|'+('target name:'+target.name).center(90,'-')+'|'
	        for lun in target.lunlist:
	            out = 'lunindex:'+lun.index+','+'blocksize:'+lun.blocksize+','+'size:'+lun.size+','+'path:'+lun.backing_store_path
	            print '|'+out.ljust(90)+'|'
	    print '|'+''.ljust(90,'-')+'|'

	def target_name2target_id(self, target_name):
		for target in self.targetlist:
			if cmp(target.name, target_name)==0:
				return target.id
		return None
	    
	def new_target(self, target_id, target_name):
	    argv = list()
	    argv=buildArguments(mode='target', op='new', tid=target_id, T=target_name)
	    (status, output) = process_call_argv(argv)
	    print output
	    if status != 0:
	        return output
	    return None

	def bind_target(self, target_id, acl='ALL'):
	    argv = list()
	    argv=buildArguments(mode='target', op='bind', tid=target_id, I=acl)
	    (status, output) = process_call_argv(argv)
	    print output
	    if status != 0:
	        return output
	    return None

	def unbind_target(self, target_id, acl='ALL'):
	    argv = list()
	    argv=buildArguments(mode='target', op='unbind', tid=target_id, I=acl)
	    (status, output) = process_call_argv(argv)
	    print output
	    if status != 0:
	        return output
	    return None

	def delete_target(self, target_id):
	    argv = list()
   	    argv=buildArguments(mode='target', op='delete', tid=target_id)
	    (status, output) = process_call_argv(argv)
	    print output
	    if status != 0:
	        return output
	    return None

	def new_lun(self, target_id, lun_index='1', path=None):
	    argv = list()
	    argv = buildArguments(mode='logicalunit', op='new', tid=target_id, lun=lun_index, b=path)
	    (status, output) = process_call_argv(argv)
	    print output
	    if status != 0:
	        return output
	    return None

	def delete_lun(self, target_id, lun_index='1'):
	    argv = list()
	    argv = buildArguments(mode='logicalunit', op='delete', tid=target_id, lun=lun_index)
	    (status, output) = process_call_argv(argv)
	    print output
	    if status != 0:
	        return output
	    return None



if __name__=="__main__":

	t=Tgt()
	print '     test begin     '.center(100,'*')
	t.reload()
	t.print_out()
	# print len(t.targetlist[0].lunlist)

	# 	for lun in target.lunlist:
	# 		print '\t', lun
	# 		for key in lun.__dict__:
	# 			print '\t\t',key,': ',lun.__dict__[key]

