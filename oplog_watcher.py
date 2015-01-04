import pymongo
from pymongo.cursor import _QUERY_OPTIONS
from pymongo.errors import AutoReconnect
import gridfs
import mongo_handler as mh
from bson.timestamp import Timestamp
import time
import os

USERNAME = "Alberto"


# Opciones del cursor.
_TAIL_OPTS = {'tailable': True, 'await_data': True}

class OplogWatcher():
	""" 
	Clase que observa el log de operaciones de la base de datos,
	utilizado por el sistema de replicas para mantener la consistencia
	entre servidores.

	Implementa un sistema de Triggers que permite replicar las operaciones
	en el cliente sin tener que preguntar continuamente a la base de datos.
	"""

	def __init__(self):
		# Conexion a la base de datos local, que contiene el oplog    
		self.db = pymongo.MongoClient().local
		# Conexion a la base de datos con la instancia de GridFS
		self.fs = gridfs.GridFS(pymongo.MongoClient().geafs)

		self.mongo_handler = mh.MongoHandler(USERNAME)
		# Alias para GridFS
		self.find_one = self.fs._GridFS__files.find_one
		self.update = self.fs._GridFS__files.update

	def tail_oplog(self):
		""" 
		Define las operaciones a observar en el oplog 
		y el bucle principal que mantiene el cursor abierto
		"""
		# Buscamos solo operaciones de insercion y actualizaciones.
		# Borrado es una actualizacion de status="active" a "deleted"
		operation = {"op":{"$in":["i","u"]}}
		# La coleccion es fs.files
		collection = {"ns" : "geafs.fs.files"}
		# Composicion de ambos requisitos
		first_query = {"$and":[operation, collection]}
		
		# Devuelve operacion mas reciente
		last_ts = self.db.oplog.rs.find(first_query).sort('$natural', -1).limit(1)[0]['ts']
		greatest_ts = {"ts":{"$gt": last_ts}}
		# Hacemos una peticion al oplog
		second_query = {"$and":[operation, collection, greatest_ts]}


		while True:
			cursor = self.db.oplog.rs.find(second_query, **_TAIL_OPTS)
			cursor.add_option(_QUERY_OPTIONS['oplog_replay'])
			try:
				while cursor.alive:
					try:
						for doc in cursor:
							self.process_op(doc)
					except StopIteration:
						time.sleep(10)
			finally:
				cursor.close()

	def process_op(self, doc):
		""" Procesa las operaciones del oplog """
		# Identifica operacion de insercion
		if doc["op"] == "i":
			#print(doc["o"]["status"])
			print("fichero creado")
			self.download_file(doc["o"]["filename"])

		# Operacion de borrado o actualizacion
		else:
			print(doc)
			filename = self.find_one(doc["o2"]["_id"])["filename"]
			if doc["o"]["$set"]["status"] == "deleted":
				self.remove_file(filename)
			elif doc["o"]["$set"]["status"] == "changed":
				self.remove_file(filename)

	def download_file(self,filename):
		"""  Composicion del metodo del mismo nombre en MongoHandler """
		self.mongo_handler.download_file(filename)

	def remove_file(self,filename):
		"""  Composicion del metodo del mismo nombre en MongoHandler """
		self.mongo_handler.remove_file(filename)

	def get_one(self, query):
		"""  Composicion del metodo del mismo nombre en MongoHandler """
		self.mongo_handler.get_one(query)
		

	


oplog = OplogWatcher()
oplog.tail_oplog()