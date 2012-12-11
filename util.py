def gethostname(mdsip):
	'mdsip: the IP address of MDS'
	import socket
	s=socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	s.connect((mdsip, 12345))
	hostname=s.getsockname()[0]
	s.close()
	return hostname



if __name__ == '__main__':
	print gethostname('www.bing.com')
