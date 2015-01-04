#!/usr/bin/python
import time
import pymongo
import gridfs
import sync_handler as sh
from watchdog.observers import Observer
from watchdog.events import RegexMatchingEventHandler

USERNAME = "Alberto"

# Try system Snapshot
class MyHandler(RegexMatchingEventHandler):
	def __init__(self):
		super(MyHandler, self).__init__(ignore_regexes=[".*\.TMP",".*~.*",".*.crdownload"])
		self.sync_handler = sh.SyncHandler(USERNAME)
		# Sincronizacion al conectar
		client_files = self.sync_handler.server_side_sync()
		self.sync_handler.client_side_sync(client_files)

	def on_created(self, event):
		if not event.is_directory:
			self.sync_handler.write_file(event.src_path, "active")
			print(event.src_path)

	def on_modified(self, event):
		if not event.is_directory:
			self.sync_handler.write_file(event.src_path, "active")

	def on_deleted(self, event):
		if not event.is_directory:
			self.sync_handler.file_deleted(event.src_path)
		if event.is_directory:
			self.sync_handler.folder_deleted(event.src_path)

	def on_moved(self, event):
		if not event.is_directory:
			self.sync_handler.change_name(event.src_path, event.dest_path)

if __name__ == "__main__":
	event_handler = MyHandler()
	observer = Observer()
	observer.schedule(event_handler, path='geafiles', recursive=True)
	observer.start()


	try:
		while True:
			time.sleep(10)
	except KeyboardInterrupt:
		observer.stop()
	observer.join()
