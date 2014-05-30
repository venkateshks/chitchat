import sys
import gevent
from gevent import server
from gevent.server import StreamServer
from gevent.monkey import patch_all; patch_all()
from multiprocessing import Process, current_process, cpu_count
import pprint
import string
from collections import defaultdict
from gevent.queue import Queue
import re
import time
from gevent.pool import Pool as GPool
from gevent.queue import Queue

users = ['U100', 'U101']

messages = ['It is a rainy day', 'I so wish it was', 'Hopefully it is working out']

address = ('0.0.0.0', 8891)

def send_msg(socket, type_of_msg, msg):
    try:
        str_to_send = type_of_msg + ' ' + string.zfill(len(msg) + 1, 4) + ' ' + msg
        print "Sending: ", str_to_send
        socket.send(str_to_send)
    except:
        pass


def test_client(user, address, to_whom):
    sock = gevent.socket.socket()
    sock.connect(address)
    send_msg(sock, 'REG', user)
    global messages
    time.sleep(1)
    for message in messages:
        print('Sending %s bytes to %s:%s' % ((len(message), ) + address))
        send_msg(sock, 'SND', to_whom + ' ' + message)
        break
    time.sleep(1)
    print "Waiting on the socket"
    data = sock.recv(8192)
    print('%s:%s: got %r' % (address + (data, )))


    
gevent.spawn(test_client, users[0], address, users[1])
gevent.spawn(test_client, users[1], address, users[0])
gevent.wait()
