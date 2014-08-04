#!/usr/bin/env python

import zmq
import zmq.auth
import time

ctx = zmq.Context.instance()
client_req = ctx.socket(zmq.REQ)
client_req.curve_publickey, client_req.curve_secretkey = zmq.auth.load_certificate('keys/client.key_secret')
client_req.curve_serverkey, _ = zmq.auth.load_certificate('keys/server.key')
server_req = ctx.socket(zmq.REP)
client_req.connect('tcp://localhost:55443')
server_req.connect('tcp://localhost:55444')

while True:
    client_req.send(b'hello')
    print client_req.recv()
    #print server_req.recv()
    #server_req.send(b'ok')
    time.sleep(1)
