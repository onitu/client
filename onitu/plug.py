#!/usr/bin/env python

import uuid
import threading
import time

import zmq
import zmq.auth
import msgpack

from logbook import Logger

from .escalator.client import Escalator
from .utils import b, u

### SPLIT PLUG ###


class _HandlerException(Exception):
    pass


class DriverError(_HandlerException):
    status_code = 1


class ServiceError(_HandlerException):
    status_code = 2


def metadata_serializer(m):
    props = [getattr(m, p) for p in m.PROPERTIES]
    return m.fid, props, m.extra


class MetadataWrapper(object):
    PROPERTIES = ('filename', 'size', 'owners', 'uptodate')

    def __init__(self, plug, fid, props, extra):
        self.plug = plug
        self.fid = fid
        for name, value in zip(self.PROPERTIES, props):
            setattr(self, name, value)
        self.extra = extra

    def write(self):
        self.plug.logger.debug('metadata:write {}', self.filename)
        self.plug.request(msgpack.packb((b'metadata write',
                                         metadata_serializer(self)),
                                        use_bin_type=True))


#class HeartBeat(threading.Thread):
#    def __init__(self, identity):
#        super(HeartBeat, self).__init__()
#        ctx = zmq.Context.instance()
#        self.socket = ctx.socket(zmq.REQ)
#        self.socket.connect('tcp://127.0.0.1:20005')
#        self.identity = identity
#
#        self._stop = threading.Event()
#
#    def run(self):
#        try:
#            while not self._stop.wait(0):
#                self._stop.wait(10)
#                self.socket.send_multipart((self.identity, b'', b'', b'ping'))
#                msg = self.socket.recv()
#                print(msg)
#        except zmq.ContextTerminated:
#            self.socket.close()
#
#    def stop(self):
#        self._stop.set()


class PlugProxy(object):
    def __init__(self):
        self.unserializers = {
            'metadata': self.metadata_unserialize
        }
        self._handlers = {}
        self.context = zmq.Context.instance()
        self.logger = None
        self.requests_socket = None
        self.handlers_socket = None
        #self.heartbeat_socket = None
        self.requests_lock = None
        self.options = {}
        self.entry_db = Escalator(self)

    def initialize(self, requests_addr, handlers_addr, options={}):
        identity = b(uuid.uuid4().hex)
        pub_key, priv_key = zmq.auth.load_certificate('keys/client.key_secret')
        server_key, _ = zmq.auth.load_certificate('keys/server.key')

        self.requests_lock = threading.Lock()

        self.requests_socket = self.context.socket(zmq.REQ)
        self.requests_socket.identity = identity
        self.requests_socket.curve_publickey = pub_key
        self.requests_socket.curve_secretkey = priv_key
        self.requests_socket.curve_serverkey = server_key
        self.requests_socket.connect(requests_addr)

        self.handlers_socket = self.context.socket(zmq.REQ)
        self.handlers_socket.identity = identity
        self.handlers_socket.curve_publickey = pub_key
        self.handlers_socket.curve_secretkey = priv_key
        self.handlers_socket.curve_serverkey = server_key
        self.handlers_socket.connect(handlers_addr)

        self.requests_socket.send_multipart((b'', b'start'))
        self.handlers_socket.send_multipart((b'', b'ready'))
        self.serv_identity, self.name = self.requests_socket.recv_multipart()
        self.name = u(self.name)

        self.logger = Logger(self.name)
        self.logger.info('Started')
        self.logger.info('Server identity - {}', self.serv_identity)

        self.options.update(options)

        #self.heartbeat = HeartBeat(identity)
        #self.heartbeat.start()

    def close(self):
        self.logger.info('Disconnecting')
        #self.heartbeat.stop()
        with self.requests_lock:
            self.requests_socket.send_multipart((b'', b'stop'))
            self.requests_socket.close()
        self.handlers_socket.close()
        self.context.term()

    def metadata_unserialize(self, m):
        return MetadataWrapper(self, *m)

    def listen(self):
        while True:
            _, msg = self.handlers_socket.recv_multipart()
            msg = msgpack.unpackb(msg, use_list=False, encoding='utf-8')
            self.logger.debug('handler {}', msg)
            cmd = self._handlers.get(msg[0], None)
            args = msg[1:]
            args = [self.unserializers.get(ser, lambda x: x)(arg)
                    for (ser, arg) in args]
            try:
                status = 0
                if cmd:
                    resp = cmd(*args)
                else:
                    resp = None
            except _HandlerException as e:
                status = e.status_code
                resp = e.args
            resp = status, resp
            self.handlers_socket.send_multipart((self.serv_identity,
                                                 msgpack.packb(resp, use_bin_type=True)))

    def request(self, msg):
        with self.requests_lock:
            self.requests_socket.send_multipart((self.serv_identity, msg))
            _, resp = self.requests_socket.recv_multipart()
            return resp

    def handler(self, name=None):
        def decorator(h):
            self._handlers[name if name is not None else h.__name__] = h
            return h
        return decorator

    def get_metadata(self, filename):
        self.logger.debug('get_metadata {}', filename)
        m = self.request(msgpack.packb(('get_metadata', filename), use_bin_type=True))
        metadata = self.metadata_unserialize(msgpack.unpackb(m, encoding='utf-8'))
        return metadata

    def update_file(self, metadata):
        self.logger.debug('update_file {}', metadata.filename)
        m = metadata_serializer(metadata)
        self.request(msgpack.packb(('update_file', m), use_bin_type=True))

    def delete_file(self, metadata):
        self.logger.debug('delete_file {}', metadata.filename)
        m = metadata_serializer(metadata)
        self.request(msgpack.packb(('delete_file', m), use_bin_type=True))

    def move_file(self, old_metadata, new_filename):
        self.logger.debug('move_file {} to {}',
                          old_metadata.filename, new_filename)
        old_m = metadata_serializer(old_metadata)
        self.request(msgpack.packb(('move_file', old_m), use_bin_type=True))


Plug = PlugProxy
