#!/usr/bin/env python

import sys
import uuid
import threading

import zmq

ctx = zmq.Context()
client_identity = uuid.uuid4().bytes
serv_identity = sys.argv[1].encode()
print serv_identity

requests = ctx.socket(zmq.REQ)
requests.identity = client_identity
requests.connect('tcp://127.0.0.1:20001')

class HandlerThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.handlers = ctx.socket(zmq.REQ)
        self.handlers.identity = client_identity
        self.handlers.connect('tcp://127.0.0.1:20003')

    def run(self):
        self.handlers.send_multipart((b'', b'ready'))
        while True:
            req_id, msg = self.handlers.recv_multipart()
            print 'HANDLER - Recv', msg, 'from', req_id
            self.handlers.send_multipart((req_id, b'ok ' + msg))

handler_thread = HandlerThread()
handler_thread.start()

while True:
    msg = raw_input().encode()
    requests.send_multipart((serv_identity, msg))
    rep_id, msg = requests.recv_multipart()
    print 'Recv', msg, 'from', rep_id
