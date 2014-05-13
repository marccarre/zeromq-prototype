import zmq
import time
import json
context = zmq.Context()
endpoint = 'tcp://127.0.0.1:1337'
server = context.socket(zmq.ROUTER)
server.identity = endpoint
server.bind(endpoint)

def reader():
    for i in xrange(0, 50, 10):
        yield range(i, i+10)
        time.sleep(1)

while True:
    try:
        address, id = server.recv_multipart()
        print(id)
        server.send_multipart([address, 'int'])
        for chunk in reader():
            server.send_multipart([address, json.dumps(chunk)])
            print('[%s] sent: %s' % (time.asctime(), chunk))
        server.send_multipart([address, 'done'])
    except KeyboardInterrupt:
        break
