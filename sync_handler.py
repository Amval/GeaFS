import os
import sys
from datetime import datetime, timedelta
import mongo_handler as mh
import hasher

FILES_DIR = "geafiles"


class SyncHandler (mh.MongoHandler):

	def __init__(self, username):
		""" Hereda los metodos de MongoHandler y ademas crea dos listas
		con los ficheros de cliente y servidor. """

		mh.MongoHandler.__init__(self, username)
		self.client_list =  self.list_client_files()
		self.server_list =  self.list_server_files()

	def list_client_files(self):
		#paths = [os.path.join(FILES_DIR,fn) for fn in next(os.walk(FILES_DIR))[2]]
		""" Devuelve SOLO los ficheros del cliente en una lista ordenada. 
		Las carpetas son ignoradas. """
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
		""" Recupera la lista de ficheros del servidor. """
		# En el servidor ya estan ordenads internamente.
		paths = self.fs.list()
		return paths

	def modification_date(self,filename):
		""" Devuelve la fecha de modificacion de un fichero del cliente. """
		t = os.path.getmtime(filename)
		return datetime.utcfromtimestamp(t)
	
	def compare_timestamps(self, filename):
		""" Dado un fichero existente en cliente y servidor, compara sus timestamps.
		La comparacion por ts no es del todo fiable, debido a los retardos propios de la
		comunicacion y las diferencias en sincronizacion.

		Podemos asumir que a distintos hashes MD5, aquel con un mayor ts habra sido
		actualizado mas recientemente.
		"""
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
		""" Compara el hash MD5 de un fichero existente tanto en el cliente como en el servidor.
		En caso de ser distintos, compara sus timestamps para resolver cual ha de ser actualizado.
		"""

		server_hash = self.find_one({"filename":filename},{"md5":True})["md5"]
		client_hash = hasher.return_hash(filename)

		if client_hash == server_hash:
			print("Ficheros iguales")
		else:
			self.compare_timestamps(filename)

	def server_side_sync(self):
		""" Recorre el listado de ficheros del servidor y compara con la lista del cliente.
		Si el fichero existe en ambas lista, compara el status del fichero en el servidor.
		- Si esta activo, compara los ficheros para detectar la copia mas reciente.
		- Si esta eliminado, borra la copia existente en el cliente.
		Si el fichero no existe en el cliente:
		- y su status es activo, lo crea
		- y su status es eliminado, significa que este es el cliente que ha iniciado el borrado,
		y por tanto no hace nada.

		Los nombres de fichero procesados se van eliminado de la lista del cliente.
		"""

		client_files = list(self.client_list)

		for filename in self.server_list:
			status = self.find_one({"filename":filename},{"status":True})["status"]
			if filename in self.client_list:
				if status == "active":
					print(filename+": Necesaria comparacion")
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
		""" Los elementos aun existentes en la lista del cliente no estan en el servidor
		Esta funcion simplemente los escribe a la base de datos.
		"""
		for filename in client_files:
			self.write_file(filename,"active")

	

#sh = SyncHandler('geafs','Alberto')
#sh._eliminate_all()
#print(sh.list_client)
#print(sh.server_list)
#sh.compare_timestamps(list_client[0], myfile)
#client_files = sh.server_side_sync()
#sh.client_side_sync(client_files)

