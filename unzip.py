import sys
import os
import tarfile
import shutil
import multiprocessing
import time


def unzip(file):

	dir = file.split('.tar')
	dir = dir[0]
	if not os.path.exists(dir):
		os.makedirs(dir)
	tar = tarfile.open(file)
	tar.extractall(dir)
	tar.close()

	print('Unzipped and closed', file)

	os.remove(file)


def zip_list(full_path):
	file_list = []
	for dirpath, dirnames, filenames in os.walk(full_path):
		for filename in [f for f in filenames if f.endswith(".tar.gz")]:
			file = os.path.join(dirpath, filename)
			if os.path.isfile(file):
				file_list.append(file)
	return file_list


def rezip(dir_name):
	shutil.make_archive(dir_name, 'zip', dir_name)
	print('Rezipped', dir_name)

path = sys.argv[1]

start = time.time()

cpu_num = multiprocessing.cpu_count()
if cpu_num > 1:
	cpu_num -= 1
	files = zip_list(path)
	
pool = multiprocessing.Pool(cpu_num)
for _ in pool.imap_unordered(unzip, files, chunksize=20):
	pass

end = time.time()

print('Finished unzipping all files.')
print('Total time in seconds: {} '.format(end - start))

rezip(path)
