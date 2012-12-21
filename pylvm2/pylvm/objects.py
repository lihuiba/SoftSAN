PETA_SUFFIX = "PB"
TERA_SUFFIX = "TB"
GIGA_SUFFIX = "GB"
MEGA_SUFFIX = "MB"
KILO_SUFFIX = "KB"
BYTE_SUFFIX = "Bytes"

class Volume:
	def __init__(self, name, path, vgname, used, attr, uuid):
		self.name = name
		self.path = path
		self.vgname = vgname
		self.used = used
		self.attr = attr
		self.uuid = uuid
		self.properties = []
		self.extent_size = 0
		self.total_extents = None
		self.allocated_extents = None
		self.free_extents = None
		self.total = None
		self.allocated = None
		self.free = None
		
	def set_extent_count(self, total, allocated):
		self.total_extents = int(total)
		self.allocated_extents = int(allocated)
		self.free_extents = self.total_extents - self.allocated_extents
		self.total = self.__build_size_string(self.total_extents)
		self.allocated = self.__build_size_string(self.allocated_extents)
		self.free = self.__build_size_string(self.free_extents)
			
	def get_sizes(self, extents):
		size_bytes = self.extent_size * extents
		size_kilos = size_bytes / 1024.0
		size_megas = size_kilos / 1024.0
		size_gigas = size_megas / 1024.0
		size_teras = size_gigas / 1024.0
		size_petas = size_teras / 1024.0
		
		return (
				size_bytes, 
				size_kilos, 
				size_megas, 
				size_gigas, 
				size_teras,
				size_petas
			)

	def __build_size_string(self, extents):
		if extents == None:
			return '0' + BYTE_SUFFIX

		(size_bytes, 
		 size_kilos, 
		 size_megas, 
		 size_gigas, 
		 size_teras,
		 size_petas
		) = self.get_sizes(extents)

		if size_petas > 1.0:
			return "%.2f" % size_petas + PETA_SUFFIX
		if size_teras > 1.0:
			return "%.2f" % size_teras + TERA_SUFFIX
		if size_gigas > 1.0:
			return "%.2f" % size_gigas + GIGA_SUFFIX
		elif size_megas > 1.0:
			return "%.2f" % size_megas + MEGA_SUFFIX
		elif size_kilos > 1.0:
			return "%.2f" % size_kilos + KILO_SUFFIX
		else:
			return str(size_bytes) + BYTE_SUFFIX
		
class VolumeGroup(Volume):
	def __init__(self, name, attr, extent_size, extents_total, extents_free, pvs_max, lvs_max):
		Volume.__init__(self, name, None, name, True, attr, None)
    
		self.extent_size = int(extent_size)
		self.set_extent_count(extents_total, extents_total - extents_free)
		self.lvs = []
		self.pvs = []
		self.max_pvs = pvs_max
		self.max_lvs = lvs_max
		
	def append_pv(self, pv):
		self.pvs.append(pv)
		
	def append_lv(self, lv):
		self.lvs.append(lv)
		

class PhysicalVolume(Volume):
	def __init__(self, name, vgname, fmt, attr, psize, pfree, initialized, total, alloc):
		Volume.__init__(self, name, name, vgname, initialized, attr, None)
		# pv properties in gigabytes
		self.extent_size = 1024 * 1024
		extents_total = float(psize) *  1024.0
		extents_free = float(pfree) * 1024.0
		self.set_extent_count(extents_total, extents_total - extents_free)
		self.format = fmt
		self.type = 0;
		if not vgname: self.type = 1
		
class LogicalVolume(Volume):
	def __init__(self, name, path, vgname, used, attrs, uuid, lv_size, ex_size):
		Volume.__init__(self, name, path, vgname, used, attrs, uuid)
		# pv properties in gigabytes
		self.extent_size = int(ex_size)
		#extents_total = int(lv_size) * int(ex_size)
		extents_total = int(lv_size)
		self.set_extent_count(extents_total, int(lv_size) * int(ex_size))
		self.segments = []
		self.snapshot_origin = None
		self.snapshot_usage = 0
		self.snapshots = []
		self.stripes = 0
		
	
