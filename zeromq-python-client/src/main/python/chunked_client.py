import zmq
import json
context = zmq.Context()
endpoint = 'tcp://127.0.0.1:1337'
client = context.socket(zmq.ROUTER)
client.connect(endpoint)

def handle(client):
    address, t = client.recv_multipart()
    print(t)
    while True:
        address, chunk = client.recv_multipart()
        if (chunk == 'done'):
            break
        for item in json.loads(chunk):
            yield item

client.send_multipart([endpoint, 'abc'])
for item in handle(client):
    print(item)
