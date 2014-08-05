#!/usr/bin/env python

import uuid
import threading

import zmq
import zmq.auth
import msgpack


DriverError = Exception
ServiceError = Exception


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
        print 'METADATA:WRITE'
        with self.plug.requests_lock:
            self.plug.requests_socket.send_multipart((self.plug.serv_identity,
                                                      msgpack.packb((b'metadata write',
                                                                     metadata_serializer(self)))))
            print repr(self.plug.requests_socket.recv_multipart())


#def metadata_unserialize(m):
#    return MetadataWrapper(*m)

#unserializers = {
#    'metadata': metadata_unserialize
#}


class PlugProxy(object):
    def __init__(self, requests_addr, handlers_addr):
        self.unserializers = {
            'metadata': self.metadata_unserialize
        }

        self._handlers = {}

        ctx = zmq.Context.instance()
        identity = uuid.uuid4().hex
        pub_key, priv_key = zmq.auth.load_certificate('keys/client.key_secret')
        server_key, _ = zmq.auth.load_certificate('keys/server.key')

        self.requests_socket = ctx.socket(zmq.REQ)
        self.requests_socket.identity = identity
        self.requests_socket.curve_publickey = pub_key
        self.requests_socket.curve_secretkey = priv_key
        self.requests_socket.curve_serverkey = server_key
        self.requests_socket.connect(requests_addr)
        self.requests_lock = threading.Lock()

        self.handlers_socket = ctx.socket(zmq.REQ)
        self.handlers_socket.identity = identity
        self.handlers_socket.curve_publickey = pub_key
        self.handlers_socket.curve_secretkey = priv_key
        self.handlers_socket.curve_serverkey = server_key
        self.handlers_socket.connect(handlers_addr)

        self.requests_socket.send_multipart((b'', b'start'))
        self.handlers_socket.send_multipart((b'', b'ready'))
        self.serv_identity, _ = self.requests_socket.recv_multipart()
        print self.serv_identity

        self.options = {'root': 'files'}

    def metadata_unserialize(self, m):
        return MetadataWrapper(self, *m)

    def listen(self):
        while True:
            _, msg = self.handlers_socket.recv_multipart()
            msg = msgpack.unpackb(msg, use_list=False)
            print 'HANDLER', msg
            cmd = self._handlers.get(msg[0].decode(), None)
            args = msg[1:]
            args = [self.unserializers.get(ser, lambda x: x)(arg) for (ser, arg) in args]
            if cmd:
                resp = cmd(*args)
            else:
                resp = None
            self.handlers_socket.send_multipart((self.serv_identity, msgpack.packb(resp)))

    def handler(self, name=None):
        def decorator(h):
            self._handlers[name if name is not None else h.__name__] = h
            return h
        return decorator

    def get_metadata(self, filename):
        print 'GET_METADATA'
        with self.requests_lock:
            self.requests_socket.send_multipart((self.serv_identity, msgpack.packb(('get_metadata', filename))))
            _, m = self.requests_socket.recv_multipart()
        metadata = self.metadata_unserialize(msgpack.unpackb(m))
        print metadata
        return metadata

    def update_file(self, metadata):
        print 'UPDATE_FILE'
        m = metadata_serializer(metadata)
        with self.requests_lock:
            self.requests_socket.send_multipart((self.serv_identity, msgpack.packb(('update_file', m))))
            self.requests_socket.recv_multipart()


#class Thread(threading.Thread):
#    def __init__(self):
#        threading.Thread.__init__(self)

#    def run(self):
#        while True:
#            msg = raw_input().encode()
#            plug.requests_socket.send_multipart((plug.serv_identity, msg))
#            rep_id, msg = plug.requests_socket.recv_multipart()
#            print 'Recv', msg, 'from', rep_id


#plug = PlugProxy('tcp://127.0.0.1:20001', 'tcp://127.0.0.1:20003')


#@plug.handler()
#def start_upload(metadata):
#    print 'START UPLOAD'
#    print metadata


#thread = Thread()
#thread.start()
#plug.listen()


class Plug(PlugProxy):
    def __init__(self):
        super(Plug, self).__init__('tcp://127.0.0.1:20001', 'tcp://127.0.0.1:20003')
