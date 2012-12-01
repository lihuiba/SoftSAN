import threading, time

class Timer(threading.Thread):
	def __init__(self, interval, func):
		threading.Thread.__init__(self)
		self.interval=interval
		self.callable=func
		self.daemon=True
		self.start()
	def delegate(self, interval, func):
		self.interval=interval
		self.callable=func
	def run(self):
		while True:
			time.sleep(self.interval)
			self.callable()
