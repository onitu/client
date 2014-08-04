#!/usr/bin/env python

# http://arslan.io/zeromq-request-slash-reply-to-a-specific-server-via-router-router-broker-in-go

import zmq

ctx = zmq.Context()

frontend_reqs = ctx.socket(zmq.ROUTER)
backend_reps = ctx.socket(zmq.ROUTER)
frontend_reps = ctx.socket(zmq.ROUTER)
backend_reqs = ctx.socket(zmq.ROUTER)
frontend_reqs.bind('tcp://*:20001')
backend_reps.bind('tcp://127.0.0.1:20002') # Get random port
frontend_reps.bind('tcp://*:20003')
backend_reqs.bind('tcp://127.0.0.1:20004') # Get random port

poller = zmq.Poller()
poller.register(frontend_reqs, zmq.POLLIN)
poller.register(backend_reps, zmq.POLLIN)
poller.register(frontend_reps, zmq.POLLIN)
poller.register(backend_reqs, zmq.POLLIN)


def handle_socket(socks, s, r, log):
    if s in socks and socks[s] == zmq.POLLIN:
        print log
        from_id, _, to_id, msg = s.recv_multipart()
        if not to_id and msg == b'ready':
            return
        print '--', from_id, to_id, msg
        r.send_multipart((to_id, b'', from_id, msg))

while True:
    socks = dict(poller.poll())
    handle_socket(socks, frontend_reqs, backend_reps, 'F-REQ')
    handle_socket(socks, backend_reps, frontend_reqs, 'B-REP')
    handle_socket(socks, frontend_reps, backend_reqs, 'F-REP')
    handle_socket(socks, backend_reqs, frontend_reps, 'B-REQ')
