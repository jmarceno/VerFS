import debugpy
debugpy.debug_this_thread()

from BTrees import IOBTree
from collections import deque
import time
from compression import compress_data

import os
import _pickle as cPickle
import json
import base64

import persistent
import transaction
import ZODB
import ZODB.FileStorage


class MyBTree(IOBTree.BTree):
    max_leaf_size = 500
    max_internal_size = 1000

class Garbage_Collector:
    def __init__(self):
        self.add_uses = deque()  # Hash of blocks that should receive an additional use counter
        self.remove_uses = deque() # Hash of blocks that should have their uses counter decreased

class DataStore:
    def __init__(self, _chunk, _chunk_size, _path):
        self.chunk = _chunk
        self.size = _chunk_size
        self.path = _path
        self.next_write_position = 0
        self.IS_FULL = False


class QueuedWrite:
    def __init__(self, _idx, _hash, _data):
        self.idx = _idx
        self.hash = _hash
        self.data = _data
        self.compressed_data = self.__compress__()
        self.chunk = 0
        self.block = 0
        self.result = False
        self.creation_time = time.time()
        self.compressed = False        

        if len(self.compressed_data) != len(self.data):
            self.compressed = True

    def __compress__(self):
        compressed, d = compress_data(self.data)
        # d = len(d).to_bytes(2, byteorder='little') + d

        return d

class Block:
    def __init__(self):
        self.chunk = 0
        self.offset = 0  # Offset dentro do chunk
        self.hash = ""
        self.size = 0  # Tamanho depois da compressao
        self.deflated_size = 0  # Tamanho sem compressao
        self.uses = 1
        self.compressed = False
        self.DELETED = False
        self.DELETION_TIME = time.time()


class FileBlock:
    def __init__(self, _hash, _size):
        self.hash = _hash
        self.size = _size


class SmallBlock:
    def __init__(self, _hash, _size, _deflated_size, _data):
        self.hash = ""
        self.size = 0  # Tamanho depois da compressao
        self.deflated_size = 0  # Tamanho sem compressao
        self.uses = 1
        self.compressed = False
        self.DELETED = False
        self.DELETION_TIME = time.time()

    
        if _size == _deflated_size:
            self.compressed = True


def write_small_block(_hash, data):
    debugpy.debug_this_thread()

    directory = os.path.join(os.getcwd(), 'metadata', 'smbs', _hash[0:2], _hash[2:4] )
    full_path = os.path.join(directory, _hash)

    if not os.path.exists(directory):
        os.makedirs(directory)
    
    # data = base64.b64encode(data)

    if _hash == '22b2b468d110349f':
        print("stop please")

    w = 0
    with open(full_path, "wb") as f:        
        w = f.write(data)
    
    return w
        # f = open(os.path.join(os.getcwd(), 'metadata', 'chunk'+str(chunk)+'.ds.vfs'), "wb")
        # f.write(b'\x00\x00\x00\x00\x00')
        # f.flush()
        # f.close()

    
def read_small_block(_hash):
    debugpy.debug_this_thread()
    directory = os.path.join(os.getcwd(), 'metadata', 'smbs', _hash[0:2], _hash[2:4] )
    full_path = os.path.join(directory, _hash)
    
    if _hash == '22b2b468d110349f':
        print("stop please")

    with open(full_path, "rb") as small_block:
        # r = small_block.read()
        # return base64.b64decode(r)
        return small_block.read()
        