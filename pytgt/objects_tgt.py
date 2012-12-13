#!/usr/bin

class Target:

    def __init__(self, id, name, lunlist=None, state='ready', driver='iscsi'):
        self.id = id
        self.name = name
        self.lunlist = lunlist or []
        self.state = state
        self.driver = driver

class Lun:

    def __init__(self, index='1', type=None, scsi_id=None, scsi_sn=None, size=None,  blocksize=None, backing_store_path='',\
        online='YES', Removable_media='NO', Prevent_removal='NO', readonly='NO',  backing_store_type='null', backing_store_flags=None):
        self.index = index 
        self.type = type or ''
        self.scsi_id = scsi_id or ''
        self.scsi_sn = scsi_sn or ''
        self.size = size or ''
        self.blocksize = blocksize or ''
        self.backing_store_path = backing_store_path or ''
        self.online = online
        self.Removable_media = Removable_media
        self.Prevent_removal = Prevent_removal
        self.Readonly = readonly
        self.backing_store_type = backing_store_type
        self.backing_store_flags = backing_store_flags or ''




