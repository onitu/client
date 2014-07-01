#!/usr/bin/env python2

import functools

import zmq
import msgpack
import pyinotify
from path import path

TMP_EXT = '.onitu-tmp'
root = path('./files')


def to_tmp(path):
    return path.parent.joinpath('.' + path.name + TMP_EXT)


def metadata_serializer(m):
    props = [getattr(m, p) for p in m.PROPERTIES]
    return m.fid, props, m.extra


class MetadataWrapper(object):
    PROPERTIES = ('filename', 'size', 'owners', 'uptodate')

    def __init__(self, fid, props, extra):
        self.fid = fid
        for name, value in zip(self.PROPERTIES, props):
            setattr(self, name, value)
        self.extra = extra

    def write(self):
        plug.server_socket.send(msgpack.packb((b'metadata write',
                                               metadata_serializer(self))))
        plug.server_socket.recv()


def metadata_unserialize(m):
    return MetadataWrapper(*m)


class PlugWrapper(object):
    def __init__(self, addr, queries_addr):
        ctx = zmq.Context.instance()
        self.handlers_socket = ctx.socket(zmq.REP)
        self.handlers_socket.connect(addr)
        self.server_socket = ctx.socket(zmq.REQ)
        self.server_socket.connect(queries_addr)
        self._handlers = {}

    def listen(self):
        try:
            while True:
                msg = msgpack.unpackb(self.handlers_socket.recv(),
                                      use_list=False)
                cmd = self._handlers.get(msg[0].decode(), None)
                resp = None
                if cmd:
                    resp = cmd(*msg[1:])
                if resp is None:
                    self.handlers_socket.send(b'merci')
                else:
                    self.handlers_socket.send(resp)
        except KeyboardInterrupt:
            self.handlers_socket.close()
            self.server_socket.close()

    def get_metadata(self, filename):
        self.server_socket.send(msgpack.packb(('get_metadata', filename)))
        m = msgpack.unpackb(self.server_socket.recv())
        metadata = metadata_unserialize(m)
        return metadata

    def update_file(self, metadata):
        m = metadata_serializer(metadata)
        self.server_socket.send(msgpack.packb(('update_file', m)))
        self.server_socket.recv()

    def handler(self, name_=None):
        def decorator(h):
            if name_:
                name = name_
            else:
                name = h.__name__
            self._handlers[name] = h
            return h
        return decorator


def unserializers(*args_unserializers):
    args_unserializers = [unser if unser is not None else lambda x: x
                          for unser in args_unserializers]

    def decorator(f):
        @functools.wraps(f)
        def wrapper(*args):
            args = [unser(arg) for (unser, arg)
                    in zip(args_unserializers, args)]
            return f(*args)
        return wrapper
    return decorator


plug = PlugWrapper('tcp://127.0.0.1:15348', 'tcp://127.0.0.1:15349')


def update_file(metadata, path, mtime=None):
    try:
        metadata.size = path.size
        metadata.extra['revision'] = mtime if mtime else path.mtime
    except (IOError, OSError):
        pass  # Report to plug
    else:
        plug.update_file(metadata)


@plug.handler()
@unserializers(metadata_unserialize)
def start_upload(metadata):
    # print('START', metadata.filename)
    filename = root.joinpath(metadata.filename)
    tmp_file = to_tmp(filename)
    try:
        if not tmp_file.exists():
            tmp_file.dirname().makedirs_p()
        tmp_file.open('wb').close()
    except IOError:
        pass  # Report to plug


@plug.handler()
@unserializers(metadata_unserialize, None, None)
def upload_chunk(metadata, offset, chunk):
    # print('UPLOAD', metadata.filename, offset, chunk)
    tmp_file = to_tmp(root.joinpath(metadata.filename))
    try:
        with open(tmp_file, 'r+b') as f:
            f.seek(offset)
            f.write(chunk)
    except (IOError, OSError):
        pass  # Report to plug


@plug.handler()
@unserializers(metadata_unserialize)
def end_upload(metadata):
    # print('END', metadata.filename)
    filename = root.joinpath(metadata.filename)
    tmp_file = to_tmp(filename)
    try:
        tmp_file.move(filename)
        mtime = filename.mtime
    except (IOError, OSError):
        pass  # Report to plug
    metadata.extra['revision'] = mtime
    metadata.write()


@plug.handler()
@unserializers(metadata_unserialize)
def abort_upload(metadata):
    # print('ABORT', metadata.filename)
    filename = root.joinpath(metadata.filename)
    tmp_file = to_tmp(filename)
    try:
        tmp_file.unlink()
    except (IOError, OSError):
        pass  # Report to plug


@plug.handler()
@unserializers(metadata_unserialize, None, None)
def get_chunk(metadata, offset, size):
    # print('GET', metadata.filename, offset, size)
    filename = root.joinpath(metadata.filename)
    try:
        with open(filename, 'rb') as f:
            f.seek(offset)
            return f.read(size)
    except (IOError, OSError):
        pass  # Report to plug


class Watcher(pyinotify.ProcessEvent):
    def process_IN_CLOSE_WRITE(self, event):
        abs_path = path(event.pathname)

        if abs_path.ext == TMP_EXT:
            return

        filename = root.relpathto(abs_path)
        metadata = plug.get_metadata(filename)
        # print(metadata_serializer(metadata))
        update_file(metadata, abs_path)


# print('Connected')

manager = pyinotify.WatchManager()
notifier = pyinotify.ThreadedNotifier(manager, Watcher())
notifier.daemon = True
notifier.start()

mask = pyinotify.IN_CREATE | pyinotify.IN_CLOSE_WRITE
manager.add_watch(root, mask, rec=True, auto_add=True)

# check_changes()
plug.listen()
notifier.stop()
