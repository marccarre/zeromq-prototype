from zmq import Context, SUB, SUBSCRIBE, UNSUBSCRIBE, ZMQError, ETERM
from threading import Thread
from time import sleep
import logging


class PushListener(Thread):
    def __init__(self, client, callback):
        Thread.__init__(self)
        self.__stopped = False
        self.__client = client
        self.__callback = callback

    def run(self):
        while not self.__stopped:
            try:
                (uuid, data) = self.__client.receive()
                self.__callback(uuid, data)
            except ZMQError as e:
                if e.errno == ETERM:
                    break  # Interrupted.
                else:
                    raise

    def stop(self):
        self.__stopped = True


class PushClient(object):
    def __init__(self):
        self.__logger = logging.getLogger('PushClient')
        self.__context = Context.instance()
        self.__socket = self.__context.socket(SUB)
        self.num_received_messages = 0L

    def connectTo(self, endpoint):
        ''' Connect to the provided endpoint '''
        self.__socket.connect(endpoint)
        sleep(0.1)  # Sleep for 100 ms, to allow for connection to come up.
        self.__logger.info('PUSH client now connected to [%s].' % endpoint)

    def subscribeTo(self, uuid):
        ''' Subscripe to the provided uuid to accept incoming messages corresponding to it. '''
        self.__socket.setsockopt(SUBSCRIBE, b'%s' % uuid)
        self.__logger.info('PUSH client now accepting data for [%s].' % uuid)

    def receive(self):
        (uuid, source, data) = self.__socket.recv_multipart()
        self.num_received_messages += 1
        self.__logger.debug('Received message #%s with key [%s] from [%s].' % (self.num_received_messages, uuid, source))
        return (uuid, data)

    def close(self):
        ''' Close client, freeing all resources being used. '''
        linger = 0  # Do NOT wait.
        self.__socket.close(linger=linger)
        self.__context.destroy(linger=linger)
        self.__context.term()
