
import zmq
import json
import traceback
import queue
from utils import load_configuration

confs = load_configuration()

def send_message(stat_msg_queue):

    server = 'tcp://'+confs['web_interface']
    retries = 0        
    context = zmq.Context()
    socket = context.socket(zmq.REQ)

    while True:
        msg = stat_msg_queue.get()        
        if type(msg) != dict:
            msg = 'Wrong data type: message provided to client was not a dict'            
        try:
            socket.connect(server)
            socket.send_json(msg)
            resp = socket.recv_json()
            if stat_msg_queue.qsize() > 499:               
                for i in range(0, 500):
                    try:
                        stat_msg_queue.get(False)
                    except queue.Empty:
                        break
                
        except:            
            continue