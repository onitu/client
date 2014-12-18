import socket


def get_open_port():
    """
    Return an URI which can be used to bind a socket to an open port.

    The port might be in use between the call to the function and its
    usage, so this function should be used with care.
    """
    tmpsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    tmpsock.bind(('127.0.0.1', 0))
    uri = 'tcp://{}:{}'.format(*tmpsock.getsockname())
    tmpsock.close()
    return uri
