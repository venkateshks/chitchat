import sys
import signal
#from gevent_zeromq import zmq
import gevent
from gevent.monkey import patch_all; patch_all()
from zmq import green as zmq
from gevent import server
from gevent.server import StreamServer
from gevent.server import _tcp_listener
from multiprocessing import Process, current_process, cpu_count
import pprint
import string
from collections import defaultdict
from gevent.queue import Queue
import re
import time
from gevent.pool import Pool as GPool
from gevent.queue import Queue

def note(format, *args):
    sys.stderr.write('[%s]\t%s\n' % (current_process().name, format%args))


listener = _tcp_listener(('0.0.0.0', 8891))

def send_bye(socket):
    socket.send('BYE 0000')

def send_msg(socket, msg):
    try:
        str_to_send = 'RCV ' + string.zfill(len(msg) + 1, 4) + ' ' + msg
#        print "Sending: ", str_to_send
        socket.send(str_to_send)
    except:
        pass

def send_msg_to_subscriber(publisher, to, msg):
    try:
#        print "Sending to PUB:" + to + '-- message is: ' + msg
        publisher.socket.send_multipart([to, msg])
    except:
        # exc_info = sys.exc_info()
        # pprint.pprint(exc_info)
        # import pdb
        # pdb.set_trace()
        pass

global_sockets = defaultdict()
global_users = defaultdict(str)    

BASE_PUBLISHER = 9000

class NothingInSocket(Exception):
    pass

class Publisher(object):
    def __init__(self, process_num, zmq_context):
        self.context = zmq_context
        self.socket = zmq_context.socket(zmq.PUB)
        print "Publishing on: ", (BASE_PUBLISHER + process_num)
        self.socket.bind("tcp://*:" + str(BASE_PUBLISHER + process_num))
        return

    def close(self):
        try:
            self.socket.close()
        except:
            pass

class Subscriber(object):
    def __init__(self, num_of_processes, zmq_context):
        self.context = zmq_context
        self.num_of_processes = num_of_processes + 1
        self.sockets = {}
        for x in xrange(self.num_of_processes):
            socket = zmq_context.socket(zmq.SUB)
            self.sockets[x] = socket
            print "Subscrbing on: ", (BASE_PUBLISHER + x)
            socket.connect("tcp://localhost:" + str(BASE_PUBLISHER + x))

        self.start_subscribers()

    def start_subscribers(self):
        processing_queue = gevent.queue.Queue(None)
        subscribers_pool = GPool(self.num_of_processes)
        subscriber_jobs = [subscribers_pool.spawn(self.start_listening_subscriber, self.sockets[x], processing_queue) for x in xrange(self.num_of_processes)]

        processing_pool = GPool(500)
        processing_jobs = [processing_pool.spawn(self.send_msg_to_user_socket, processing_queue) for x in xrange(500)]
        

    def send_msg_to_user_socket(self, processing_queue):
        while True:
            job = processing_queue.get()
            if job == StopIteration:
                processing_queue.put(StopIteration)
                print "Exiting one thread"
                return
            send_msg(job[0], job[1])

        
    def start_listening_subscriber(self, socket, processing_queue):
        while True:
#            print "Waiting on the SUB port"
            try:
                op = socket.recv_multipart()
#                print "Got Something"
                to_whom = op[0]
                content = op[1]
 #               print "Got Message into subscriber: ", to_whom, content
                if to_whom in global_sockets.keys():
                    processing_queue.put((global_sockets[to_whom], content))
#                send_msg(global_sockets[to_whom], content)
            except:
                # exc_info = sys.exc_info()
                # pprint.pprint(exc_info)
                # import pdb
                # pdb.set_trace()
                pass

    def set_subscriber(self, to_whom):
        for x in xrange(self.num_of_processes):
#            self.sockets[x].setsockopt(zmq.SUBSCRIBE, '')
            self.sockets[x].setsockopt(zmq.SUBSCRIBE, to_whom.lower().strip())

    def close(self):
        for x in xrange(self.num_of_processes):
            try:
                self.sockets[x].close()
            except:
                pass


class ListeningServer(StreamServer):
    def __init__(self, listener, publisher, subscriber):
        StreamServer.__init__(self, listener, self.proto)
        self.publisher = publisher
        self.subscriber = subscriber

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
#                print "Command String is: ", current_process().name, command_str
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
                    self.subscriber.set_subscriber(rest_str)
                elif command == 'snd':
                    try:
                        msg_parts = rest_str.split(' ')
                        to_whom = msg_parts[0].lower()
                        the_msg = ' '.join(msg_parts[1:])
                        send_msg_to_subscriber(self.publisher, to_whom, global_users[socket] + ' ' + string.zfill(len(msg_parts) - 1, 4) + ' ' + the_msg)
#                        send_msg(global_sockets[to_whom], global_users[socket] + ' ' + string.zfill(len(msg_parts) - 1, 4) + ' ' + the_msg)
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


number_of_processes = 9

    

def handle_signals(listening_server, publisher, subscriber):
    print "Closing Everything"
    listening_server.close()
    publisher.close()
    subscriber.close()

def serve_forever(listener, process_num):
    note('starting server')
    zmq_context = zmq.Context()
    publisher = Publisher(process_num, zmq_context)
    time.sleep(1)
    subscriber = Subscriber(number_of_processes, zmq_context)
    print "Sarting the listening server"
    listening_server = ListeningServer(listener, publisher, subscriber)
    gevent.signal(signal.SIGTERM,  handle_signals, listening_server, publisher, subscriber)
    gevent.signal(signal.SIGINT, handle_signals, listening_server, publisher, subscriber)

    # gevent.signal(signal.SIGTERM,  listening_server.close)
    # gevent.signal(signal.SIGINT, listening_server.close)
    listening_server.start()
    gevent.wait()
    
print 'Starting %s processes' % number_of_processes
for i in range(number_of_processes):
    Process(target=serve_forever, args=(listener,i + 1,)).start()

serve_forever(listener, 0)


#go run ./test.go -clients=10000 -burst_interval=100 -burst=100 -port="8891" -duration=1000 -wait=10000
