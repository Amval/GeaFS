from multiprocessing import Process, Manager
from watchdog.observers import Observer
from watchdog.events import RegexMatchingEventHandler
import file_watcher as fw
import oplog_watcher as ow
import time


def f(fw_oplog, ow_oplog):
	event_handler = fw.ClientHandler(fw_oplog, ow_oplog)
	observer = Observer()
	observer.schedule(event_handler, path='geafiles', recursive=True)
	observer.start()


	try:
		while True:
			time.sleep(10)
	except KeyboardInterrupt:
		observer.stop()
	observer.join()


def g(fw_oplog, ow_oplog):
	#while True:
	#	if "geafiles/jarl" in fw_oplog:
	#		print("Operacion encontrada")
	#	else:
	#		ow_oplog.append("jarl")
	#	print(ow_oplog)
	#	time.sleep(10)
	oplog = ow.OplogWatcher(fw_oplog,ow_oplog)
	oplog.tail_oplog()



if __name__ == '__main__':
	manager = Manager()

	fw_oplog = manager.list()
	ow_oplog = manager.list()

	p1 = Process(target=f, args=(fw_oplog, ow_oplog))
	p1.start()
	
	p2 = Process(target=g, args=(fw_oplog, ow_oplog))
	p2.start()

	p1.join()
	p2.join()

