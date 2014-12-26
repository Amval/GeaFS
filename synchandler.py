import os
import sys
from datetime import datetime, timedelta
import mongohandler as mh
import hasher

FILES_DIR = "geafiles"


class SyncHandler (mh.MongoHandler):

	def __init__(self, db):

		mh.MongoHandler.__init__(self,db)
		self.client_list =  self.list_client_files()
		self.server_list =  self.list_server_files()

	def list_client_files(self):
		# MAL. Explorar subdirectorios
		#paths = [os.path.join(FILES_DIR,fn) for fn in next(os.walk(FILES_DIR))[2]]
		ordered_paths = []
		paths = []
		for path, subdirs, files in os.walk(FILES_DIR):
			for filename in files:
				f = os.path.join(path,filename)
				paths.append(f)
		# Devuelve nueva lista ordenada. Sort no funciona bien si fueran cadenas en unicode.
		# Solucion, si fuera necesaria: http://stackoverflow.com/questions/36139/how-do-i-sort-a-list-of-strings-in-python
		for x in sorted(paths):
			ordered_paths.append(x)
		return ordered_paths

	def list_server_files(self):
		# En el servidor ya estan ordenads internamente.
		paths = self.fs.list()
		return paths

	def modification_date(self,filename):
		t = os.path.getmtime(filename)
		return datetime.utcfromtimestamp(t)
	
	def compare_timestamps(self, filename):
		server_file = self.find_one({"filename":filename},{"uploadDate":True,"status":True}) 

		server_ts = server_file["uploadDate"]
		client_ts = self.modification_date(filename)

		if server_ts > client_ts:
			print("Actualiza cliente")
			if server_file["status"] == "active":
				self.download_file(filename)
			elif server_file["status"] == "deleted":
				self.remove_file(filename)
		else:
			print("Actualiza servidor")
			self.write_file(filename,"active")

	def compare_hashes(self, filename):
		server_hash = self.find_one({"filename":filename},{"md5":True})["md5"]
		client_hash = hasher.return_hash(filename)

		if client_hash == server_hash:
			print("Ficheros iguales")
		else:
			self.compare_timestamps(filename)

	def server_side_sync(self):

		client_files = list(self.client_list)

		for filename in self.server_list:
			status = self.find_one({"filename":filename},{"status":True})["status"]
			if filename in self.client_list:
				print(filename+": Necesaria comparacion")
				if status == "active":
					self.compare_hashes(filename)
					client_files.remove(filename)
				else:
					self.remove_file(filename)
			elif filename not in self.client_list:
				if status == "active":
					print(filename+": Crear fichero en cliente")
					self.download_file(filename)

		return client_files

	def client_side_sync(self, client_files):
		for filename in client_files:
			self.write_file(filename,"active")

	def download_file(self,filename):
		path = filename.split("/")
		if len(path) > 2:
			path.pop()
			path = "/".join(path)
			try:
				os.makedirs(path)
			except OSError:
				print("Ignorando la creacion de {0} porque ya existe".format(path))

		db_file = self.get_one({"filename":filename})
		with open(filename,"wb") as f_out:
			f_out.write(db_file.read())

	def remove_file(self,filename):
		os.remove(filename)
		path = filename.split("/")
		if len(path) > 2:
			path.pop()
			path = "/".join(path)
			try:
				os.removedirs(path)
			except OSError:
				print("No es posible borrar el directorio {0} porque no esta vacio.".format(path))
		
		print("Eliminado: {0}".format(filename))



sh = SyncHandler('geafs')
#sh._eliminate_all()
#print(sh.list_client)
print(sh.server_list)
#sh.compare_timestamps(list_client[0], myfile)
client_files = sh.server_side_sync()
sh.client_side_sync(client_files)

