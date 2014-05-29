import sys
import signal
import gevent
from gevent import server
from gevent.server import StreamServer
from gevent.server import _tcp_listener
from gevent.monkey import patch_all; patch_all()
from multiprocessing import Process, current_process, cpu_count
import pprint
import string
from collections import defaultdict
from gevent.queue import Queue

def note(format, *args):
    sys.stderr.write('[%s]\t%s\n' % (current_process().name, format%args))


listener = _tcp_listener(('0.0.0.0', 8891))

def send_bye(socket):
    socket.send('BYE 0000')

def send_msg(socket, msg):
    try:
        str_to_send = 'RCV ' + string.zfill(len(msg) + 1, 4) + ' ' + msg
        #    print "Sending: ", str_to_send
        socket.send(str_to_send)
    except:
        pass

global_sockets = defaultdict()
global_users = defaultdict(str)    

class NothingInSocket(Exception):
    pass

class ListeningServer(StreamServer):

    def __init__(self, listener):
        StreamServer.__init__(self, listener, self.proto)

    def wait_till_as_many_bytes(self, socket, as_many_bytes):
        command_str = ''
        command_size = as_many_bytes
        while True:
            try:
                command_str += socket.recv(command_size - len(command_str))
            except Exception as e:
                raise e
            if command_str == '':
#                print "Socket Returned nothing"
                raise NothingInSocket("Socket Returned nothing")
            if len(command_str) == as_many_bytes:
                return command_str

    def proto(self, socket, address):
        while True:
            try:
                command_str = self.wait_till_as_many_bytes(socket, 8)
#                print "Command String is: ", command_str
                (command, length_of_rest) = command_str.lower().split(' ')
                length_of_rest = int(length_of_rest)
                try:
                    rest_str = self.wait_till_as_many_bytes(socket, length_of_rest)
                    rest_str = rest_str.strip()
                except:
                    exc_info = sys.exc_info()
                    pprint.pprint(exc_info)
                    socket.close()
                    break
#                print "REST is: ", rest_str
                if command == 'reg':
                    rest_str = rest_str.lower()
                    global_sockets[rest_str] = socket
                    global_users[socket] = rest_str
                elif command == 'snd':
                    try:
                        # import pdb
                        # pdb.set_trace()
                        msg_parts = rest_str.split(' ')
                        to_whom = msg_parts[0].lower()
                        the_msg = ' '.join(msg_parts[1:])
                        send_msg(global_sockets[to_whom], global_users[socket] + ' ' + string.zfill(len(msg_parts) - 1, 4) + ' ' + the_msg)
#                        gevent.spawn(send_msg, global_sockets[to_whom], global_users[socket] + ' ' + string.zfill(len(msg_parts) - 1, 4) + ' ' + the_msg)
                    except:
                        pass
                else:
                    socket.close()
                    break
            except NothingInSocket as e:
#                print e
                socket.close()
                return
            except:
#                exc_info = sys.exc_info()
#                pprint.pprint(exc_info)
                socket.close()
                return

    def echo(self, socket, address):
        print 'New connection from %s:%s' % address
        fileobj = socket.makefile()
        # fileobj.write('Welcome to the echo server! Type quit to exit.\r\n')
        # fileobj.write('In %s\r\n' % current_process().name)
        # fileobj.flush()
        while True:
            try:
                line = fileobj.readline()
            except:
                exc_info = sys.exc_info()
                pprint.pprint(exc_info)
            if not line:
                print "client disconnected"
                break
            if line.strip().lower() == 'quit':
                print "client quit"
                break
            # fileobj.write(current_process().name + '\t' + line)
            # fileobj.flush()
            print "echoed", repr(line)

        
    def close(self):
        if self.closed:
            sys.exit('Multiple exit signals received - aborting.')
        else:
            note('Closing socket')
            send_bye(self.socket)
            StreamServer.close()


def serve_forever(listener):
    note('starting server')
    listening_server = ListeningServer(listener)
    gevent.signal(signal.SIGTERM,  listening_server.close)
    gevent.signal(signal.SIGINT, listening_server.close)
    listening_server.start()
    gevent.wait()
    
number_of_processes = 0
print 'Starting %s processes' % number_of_processes
for i in range(number_of_processes):
    Process(target=serve_forever, args=(listener,)).start()

serve_forever(listener)
