import os

# the blockdevice information about a target 
# can be found in directory'/sys/class/iscsi_session/' 
#
# iscsi_session
#      |
#     -----------------------------
#     |          |      .....     |
# session1    session2  ..... sessionN
#     |----------
#     |         |
#  device   targetname #here check the targetname
#     |
# targetNo:0:0
#     |
# No:0:0:LunNo
#     |
#   block
#     |
# blockdeviceName
def check_targetname(targetfile_path, target):
	targetfile = file(targetfile_path).readlines()
	for name in targetfile:
		if name.find(target) > -1:
			return True
	return False

def get_blockdev_by_targetname(target):
	path = '/sys/class/iscsi_session/'
	for session in os.listdir(path):
		if session.find('session') > -1:
			path_session = path+session+'/'
			if check_targetname(path_session+'targetname', target) is True:
				path_session_dev = path_session + 'device/'
				for tar in os.listdir(path_session_dev):
					if tar.find('target') > -1:
						path_session_dev_tar = path_session_dev+tar+'/'
						for fin in os.listdir(path_session_dev_tar):
							if fin.find(':0:0:') > -1 and fin.find(':0:0:0') is -1:
								path_session_dev_tar_final = path_session_dev_tar+fin+'/block/'
								while os.path.isdir(path_session_dev_tar_final) is False:
									pass
								device = os.listdir(path_session_dev_tar_final)
								return device[0];
								
	print 'target not found'