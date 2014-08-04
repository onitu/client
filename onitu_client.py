#!/usr/bin/env python

import uuid
import threading

import zmq

ctx = zmq.Context()
client_identity = uuid.uuid4().hex

requests = ctx.socket(zmq.REQ)
requests.identity = client_identity
requests.connect('tcp://127.0.0.1:20001')

requests.send_multipart((b'', b'start'))
serv_identity = requests.recv()
print serv_identity

while True:
    msg = raw_input().encode()
    requests.send_multipart((serv_identity, msg))
    rep_id, msg = requests.recv_multipart()
    print 'Recv', msg, 'from', rep_id
