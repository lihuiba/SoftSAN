def gethostname(mdsip):
	import socket
	'mdsip: the IP address of MDS'
	s=socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	s.connect((mdsip, 12345))
	hostname=s.getsockname()[0]
	s.close()
	return hostname
