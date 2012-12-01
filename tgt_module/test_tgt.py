from tgt_ctrl import *

if __name__ == '__main__':
	tgt = Tgt()
	# # tgt_ctrl.reload()
	# tgt_ctrl.new_target('11','test:iscsi')
	# tgt_ctrl.bind_target('11','ALL')
	# tgt_ctrl.new_lun('11','1','/dev/vg0/lv0')
	tgt.reload()
	tgt.print_out()