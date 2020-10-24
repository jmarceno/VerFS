#
#   Hello World client in Python
#   Connects REQ socket to tcp://localhost:5555
#   Sends "Hello" to server, expects "World" back
#

import zmq
import json

def send_message(stat_msg_queue):

    server = "tcp://localhost:5555"
    retries = 0        
    context = zmq.Context()
    socket = context.socket(zmq.REQ)

    while True:
        msg = stat_msg_queue.get()
        if type(msg) != dict:
            msg = "Wrong data type: message provided to client was not a dict"        
        try:
            socket.connect(server)
            socket.send_json(msg)
            resp = socket.recv_json()
        except:
            pass