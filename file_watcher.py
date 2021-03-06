#!/usr/bin/python
import time
import pymongo
import gridfs
import sync_handler as sh
from watchdog.observers import Observer
from watchdog.events import RegexMatchingEventHandler

USERNAME = "Alberto"

# Try system Snapshot
class ClientHandler(RegexMatchingEventHandler):
	def __init__(self, fw_oplog, ow_oplog):
		super(ClientHandler, self).__init__(ignore_regexes=[".*\.TMP",".*~.*",".*.crdownload",".*/\..*"])
		self.sync_handler = sh.SyncHandler(USERNAME)
		# 
		self.fw_oplog = fw_oplog
		self.ow_oplog = ow_oplog
		# Sincronizacion al conectar
		client_files = self.sync_handler.server_side_sync()
		self.sync_handler.client_side_sync(client_files)

	def on_created(self, event):
		""" Procesa el evento de creacion de un fichero en el cliente y lo escribe
		en la base de datos. Los directorios vacios no son sincronizados. """

		if not event.is_directory:
			if event.src_path not in self.ow_oplog:
				self.sync_handler.write_file(event.src_path, "active")
				self.fw_oplog.append(event.src_path)

			else:
				self.ow_oplog.remove(event.src_path)
		#print("fw_oplog: {0}".format(self.fw_oplog))
		#print("ow_oplog: {0}".format(self.ow_oplog))

	def on_modified(self, event):
		""" Procesa el evento de modificacion de un fichero en el cliente y lo escribe
		en la base de datos. """
		# Carpetas modificadas!!
		if not event.is_directory:
			if event.src_path not in self.ow_oplog:
				self.sync_handler.write_file(event.src_path, "active")
				self.fw_oplog.append(event.src_path)
			else:
				self.ow_oplog.remove(event.src_path)
		print("fw_oplog: {0}".format(self.fw_oplog))
		print("ow_oplog: {0}".format(self.ow_oplog))


	def on_deleted(self, event):
		""" Procesa el evento de borrado de un fichero o carpeta en el cliente y notifica
		a la base de datos. """
		if not event.is_directory:
			self.sync_handler.file_deleted(event.src_path)
		if event.is_directory:
			self.sync_handler.folder_deleted(event.src_path)

	def on_moved(self, event):
		""" Procesa el evento de cambio de ruta de un fichero, que equivale a su cambio
		de nombre. En la base de datos no se almacenan carpetas, asi que la ruta relativa
		a geafiles de un fichero es utilizada como nombre del fichero.
		Ej: geafiles/ej.txt => geafiles/ejemplo/ej.txt 
		"""
		if not event.is_directory:
			if event.src_path not in self.ow_oplog:
				self.sync_handler.change_name(event.src_path, event.dest_path)
				self.fw_oplog.append(event.src_path)
			else:
				self.ow_oplog.remove(event.src_path)


#if __name__ == "__main__":
#	event_handler = ClientHandler()
#	observer = Observer()
#	observer.schedule(event_handler, path='geafiles', recursive=True)
#	observer.start()


#	try:
#		while True:
#			time.sleep(10)
#	except KeyboardInterrupt:
#		observer.stop()
#	observer.join()
