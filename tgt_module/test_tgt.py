from tgt_ctrl import *

if __name__ == '__main__':
	tgt = Tgt()
	target_id = '4'
	target_name = 'iqn:test'
	acl = 'ALL'
	lun_index = '1'
	path = '/dev/VolGroup/lvtest'

	# test new target, new lun, bind target
	# tgt.new_target(target_id, target_name)
	# tgt.bind_target(target_id, acl)
	# tgt.new_lun(target_id, lun_index, path)

	tgt.reload()
	tgt.print_out()
	out = tgt.target_name2target_id('iqn:test')
	if out==None:
		print 'no cmp'
	else:
		print 'id = '+out

	# test path2target_id
	# print tgt.path2target_id(path)
