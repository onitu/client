#!/usr/bin/env python2

import functools

import zmq
import msgpack

port = 15348

ctx = zmq.Context.instance()
serv = ctx.socket(zmq.SUB)
serv.connect('tcp://127.0.0.1:{}'.format(port))
serv.setsockopt(zmq.SUBSCRIBE, b'')

class MetadataWrapper(object):
    PROPERTIES = ('filename', 'size', 'owners', 'uptodate')

    def __init__(self, fid, props, extra):
        self.fid = fid
        for name, value in zip(self.PROPERTIES, props):
            setattr(self, name, value)
        self.extra = extra

def metadata_unserialize(m):
    return MetadataWrapper(*m)

class PlugWrapper(object):
    def __init__(self):
        self.handlers = {}

    def handler(self, name_=None):
        def decorator(h):
            if name_:
                name = name_
            else:
                name = h.__name__
            self.handlers[name] = h
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

plug = PlugWrapper()

@plug.handler()
@unserializers(metadata_unserialize)
def start_upload(metadata):
    print(metadata)

@plug.handler()
@unserializers(metadata_unserialize, None, None)
def upload_chunk(metadata, offset, chunk):
    print(metadata, offset, chunk)

@plug.handler()
@unserializers(metadata_unserialize)
def end_upload(metadata):
    print(metadata)

@plug.handler()
@unserializers(metadata_unserialize)
def abort_upload(metadata):
    print(metadata)

print('Connected')

try:
    while True:
        msg = msgpack.unpackb(serv.recv(), use_list=False)
        cmd = plug.handlers.get(msg[0].decode(), None)
        if cmd:
            cmd(*msg[1:])
except KeyboardInterrupt:
    serv.close()
