def test_tgt():
	t=Tgt()
	print '     test begin     '.center(100,'*')
	print 
	tgt = Tgt()
	import random,time
	target_id = str(random.randint(20,30))
	target_name = 'iqn:test'+str(time.time())
	acl = 'ALL'
	lun_index = '3'
	lun_path = '/dev/VolGroup/lvtest'

	# test new target, new lun, bind target
	# tgt.new_target(target_id, target_name)
	# tgt.bind_target(target_id, acl)
	# tgt.new_lun(target_id, lun_index, path)
	tgt.assemble(target_id, target_name, lun_path)

	# for i in range(20,31):
	# 	target_id = str(i)
	# 	tgt.delete_target(target_id)
	
	tgt.reload()
	tgt.print_out()
	

	print 
	print '     test end     '.center(100,'*')


if __name__=="__main__":
	test_tgt()
	