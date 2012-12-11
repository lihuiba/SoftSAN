#!/usr/bin

class Target:

    def __init__(self, id, name, lunlist=None, acl='ALL', state='ready', driver='iscsi', account=list()):
        self.id = id
        self.name = name
        self.lunlist = lunlist or []
        self.acl = acl
        self.state = state
        self.driver = driver
        self.account = account

class Lun:

    def __init__(self, index='0', type='controller', scsi_id='IET     000x000y', scsi_sn='beaf00', size=0,  blocksize=1, backing_store_path=None,\
        online=True, Removable_media=False, Prevent_removal=False, readonly=False,  backing_store_type='null', backing_store_flags=None):
        self.index = index
        self.type = 'type'
        self.scsi_id = scsi_id
        self.scsi_sn = scsi_sn
        self.size = size
        self.blocksize = blocksize
        self.backing_store_path = backing_store_path
        self.online = online
        self.Removable_media = Removable_media
        self.Prevent_removal = Prevent_removal
        self.readonly = readonly
        self.backing_store_type = backing_store_type
        self.backing_store_flags = backing_store_flags




