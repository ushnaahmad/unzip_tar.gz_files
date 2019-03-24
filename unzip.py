import sys
import os
import tarfile
import shutil
import Queue as queue
import threading
import multiprocessing
import hashlib
import time

start = time.time()

class Unzip:
	def __init__(self, path):
		self.path = path
		self.files = []
		self.unzip_queue = queue.Queue()

	def unzip(self):
		cpu_num = multiprocessing.cpu_count()
		if cpu_num > 1:
			cpu_num -= 1
		for x in range(cpu_num):
			worker = Unzip.UnzippingTask(self)
			worker.daemon = True
			worker.start()
		for dirpath, dirnames, filenames in os.walk(self.path):
			for filename in [f for f in filenames if f.endswith(".tar.gz")]:
				self.file = os.path.join(dirpath, filename)
				if os.path.isfile(self.file):
					self.files.append(self.file)
					self.unzip_queue.put(self.file)
				self.unzip_queue.join()

	class UnzippingTask(threading.Thread):
		def __init__(self, Unzip):
			threading.Thread.__init__(self)
			self.unzip = Unzip
			self.unzip_queue = Unzip.unzip_queue


		def run(self):
			while True:
				file = self.unzip_queue.get()

				dir = file.split('.tar')
				dir = dir[0]
				if not os.path.exists(dir):
					os.makedirs(dir)
				tar = tarfile.open(file)
				tar.extractall(dir)
				tar.close()

				print('Unzipped and closed', file)

				os.remove(file)
				self.unzip_queue.task_done()

def rezip(dir_name):
	shutil.make_archive(dir_name, 'zip', dir_name)
	print('Rezipped', dir_name)


path = sys.argv[1]

data = Unzip(path)
data.unzip()
end = time.time()
print('Finished unzipping all files.')
print('Total time in seconds: {} '.format(end - start))

rezip(path)
