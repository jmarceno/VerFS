# import debugpy
# debugpy.debug_this_thread()

import traceback
from BTrees import IOBTree
from collections import deque
import time
from compression import compress_data

import os
import _pickle as cPickle
import json
import base64

# import persistent
# import transaction
# import ZODB
# import ZODB.FileStorage


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


class File_Inode:
    def __init__(self, _id):
        self.id = _id
        self.uid = 0
        self.gid = 0
        self.mode = 0
        self.mtime_ns = time.time_ns()
        self.atime_ns = time.time_ns()
        self.ctime_ns = time.time_ns()
        self.target = ""
        self.size = 0
        self.rdev = 0
        self.data = [] # List of FileBlock 's


class Directory_Inode:
    def __init__(self, inode):        
        # self.row_id = row_id
        self.name = ""
        self.inode = inode
        self.parent_inode = None
    

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


def write_small_block(_hash, data, stat_msg_queue):
    # debugpy.debug_this_thread()

    directory = os.path.join(os.getcwd(), 'metadata', 'smbs', _hash[0:2], _hash[2:4] )
    full_path = os.path.join(directory, _hash)

    if not os.path.exists(directory):
        try:
            os.makedirs(directory)
        except FileExistsError:
            pass
    
    w = 0
    with open(full_path, "wb") as f:        
        w = f.write(data)
    
    return w        


def read_small_block(_hash, stat_msg_queue):
    # debugpy.debug_this_thread()
    directory = os.path.join(os.getcwd(), 'metadata', 'smbs', _hash[0:2], _hash[2:4] )
    full_path = os.path.join(directory, _hash)

    with open(full_path, "rb") as small_block:
        return small_block.read()


def delete_small_block(_hash, stat_msg_queue):
    # debugpy.debug_this_thread()
    directory = os.path.join(os.getcwd(), 'metadata', 'smbs', _hash[0:2], _hash[2:4] )
    full_path = os.path.join(directory, _hash)

    try:
        os.remove(full_path)
        return True
    except Exception:
        print(traceback.format_exc())
        return False
    