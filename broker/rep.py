#!/usr/bin/env python

import sys
import threading
import time

import zmq

ctx = zmq.Context()
identity = sys.argv[1].encode()
print identity

client_requests = ctx.socket(zmq.REQ)
client_requests.identity = identity
client_requests.connect('tcp://127.0.0.1:20002')

client_requests.send_multipart((b'', b'ready'))

class HandlerThread(threading.Thread):
    def __init__(self, client_identity):
        threading.Thread.__init__(self)
        self.client_identity = client_identity
        self.handlers = ctx.socket(zmq.REQ)
        self.handlers.identity = identity
        self.handlers.connect('tcp://127.0.0.1:20004')

    def run(self):
        n = 1
        while True:
            time.sleep(3)
            self.handlers.send_multipart((self.client_identity, 'Coucou{}'.format(n).encode()))
            rep_id, msg = self.handlers.recv_multipart()
            print 'HANDLER - Recv', msg, 'from', rep_id
            n += 1

handler_thread = None

while True:
    req_id, msg = client_requests.recv_multipart()
    print 'Recv', msg, 'from', req_id
    client_requests.send_multipart((req_id, b'ok ' + msg))
    if handler_thread is None:
        handler_thread = HandlerThread(req_id)
        handler_thread.start()
