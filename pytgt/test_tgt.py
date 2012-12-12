from tgt_ctrl import *

if __name__ == '__main__':
	tgt = Tgt()
	import random,time
	target_id = str(random.randint(20,30))
	target_name = 'iqn:test'+str(time.time())
	acl = 'ALL'
	lun_index = '2'
	path = '/dev/VolGroup/lvtest'

	# test new target, new lun, bind target
	tgt.new_target(target_id, target_name)
	tgt.bind_target(target_id, acl)
	tgt.new_lun('30', lun_index, path)

	for i in range(20,31):
		target_id = str(i)
		tgt.delete_target(target_id)
	tgt.reload()
	tgt.print_out()
	
	# out = tgt.target_name2target_id('iqn:test')
	# if out==None:
	# 	print 'no cmp'
	# else:
	# 	print 'id = '+out

	