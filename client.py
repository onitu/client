#!/usr/bin/env python2

import zmq
from time import sleep
from threading import Thread
import os.path
import pyinotify

from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler

host, port = '127.0.0.1', 43364
root = 'files'

if not os.path.exists(root):
    os.makedirs(root)

context = zmq.Context()
req = context.socket(zmq.REQ)
rep = context.socket(zmq.REP)
req.connect('tcp://{}:{}'.format(host, port))

#nowatch_files = set()
nowatch_files = {}

class Watcher(pyinotify.ProcessEvent):
    def process_IN_CREATE(self, event):
        return self.handle(event)

    def process_IN_CLOSE_WRITE(self, event):
        return self.handle(event)

    def handle(self, event):
        filename = os.path.normpath(os.path.relpath(event.pathname, '.'))
        mtime = os.path.getmtime(event.pathname)
        #print(nowatch_files, mtime)
        if filename in nowatch_files and mtime <= nowatch_files[filename]:
            #print('###########')
            #nowatch_files.remove(filename)
            return
        filename = os.path.normpath(os.path.relpath(filename, root))
        print filename
        req.send_multipart(('file_updated',
                            filename,
                            str(os.path.getsize(event.pathname)),
                            str(mtime)))
        #req.send_multipart(('file_updated',
        #                    filename,
        #                    str(os.path.getsize(event.pathname))))
        print req.recv_multipart()

class EventHandler(FileSystemEventHandler):
    events = 0

    def on_moved(self, event):
        def handle_move(event):
            if event.is_directory:
                return
            self._handle_update(event.dest_path)

        handle_move(event)
        if event.is_directory:
            for subevent in event.sub_moved_events():
                handle_move(subevent)

    def on_modified(self, event):
        if event.is_directory:
            return
        self._handle_update(event.src_path)

    def _handle_update(self, abs_filename):
        filename = os.path.normpath(os.path.relpath(abs_filename, '.'))
        #if filename in nowatch_files:
        #    #nowatch_files.remove(filename)
        #    return
        filename = os.path.normpath(os.path.relpath(filename, root))
        print filename
        req.send_multipart(('file_updated',
                            filename,
                            str(os.path.getsize(abs_filename)),
                            str(os.path.getmtime(abs_filename))))
        print req.recv_multipart()

manager = pyinotify.WatchManager()
notifier = pyinotify.ThreadedNotifier(manager, Watcher())
notifier.start()
mask = pyinotify.IN_CREATE | pyinotify.IN_CLOSE_WRITE
manager.add_watch(root, mask, rec=True)

#observer = Observer()
#observer.schedule(EventHandler(), path=root, recursive=True)
#observer.start()

req.send_multipart(('connect',))
_, port2 = req.recv_multipart()
rep.connect('tcp://{}:{}'.format(host, port2))

while True:
    msg = rep.recv_multipart()
    print 'msg', msg
    try:
        cmd = msg[0]
        if cmd == 'start_transfert':
            _, filename = msg
            #nowatch_files.add(filename)
            rep.send_multipart(('ok',))
        elif cmd == 'end_transfert':
            _, filename = msg
            #nowatch_files.remove(filename)
            #del nowatch_files[filename]
            rep.send_multipart(('ok',))
        elif cmd == 'read_chunk':
            _, filename, offset, size = msg
            offset = int(offset)
            size = int(size)
            try:
                if '..' in filename:
                    raise IOError
                filename = os.path.join(root, filename)
                with open(filename, 'rb') as f:
                    f.seek(offset)
                    rep.send_multipart(('ok', f.read(size)))
            except:
                rep.send_multipart(('ko', 'file not found', filename))
        elif cmd == 'write_chunk':
            _, filename, offset, chunk = msg
            if '..' in filename:
                raise IOError
            filename = os.path.join(root, filename)
            #nowatch_files.add(filename)
            offset = int(offset)
            dirname = os.path.dirname(filename)
            mode = 'rb+'
            if not os.path.exists(filename):
                if dirname and not os.path.exists(dirname):
                    os.makedirs(dirname)
                mode = 'wb+'
            with open(filename, mode) as f:
                f.seek(offset)
                f.write(chunk)
                nowatch_files[filename] = os.path.getmtime(filename)
            rep.send_multipart(('ok',))
        else:
            rep.send_multipart(('ok',))
    except:
        rep.send_multipart(('ko',))
