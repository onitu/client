#!/usr/bin/env python2

import zmq

port = 15348

ctx = zmq.Context.instance()
serv = ctx.socket(zmq.SUB)
serv.connect('tcp://127.0.0.1:{}'.format(port))
serv.setsockopt(zmq.SUBSCRIBE, b'')

print('Connected')

while True:
    print(serv.recv())
