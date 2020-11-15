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

from sqlitedict import SqliteDict
from os import path

import lmdb

max_map_size = (1073741824*1024)
small_block_db_path = os.path.join(os.getcwd(), '..', '..', 'metadata', "small_blocks")


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
        self.inode = _id        
        self.parent_inode = 0
        self.name = ""
        self.target = ""
        self.st_nlink = 1
        self.list_on_dir_lookup = True
        self.lookup_count = 1
        self.uid = 0
        self.gid = 0
        self.mode = 0
        self.mtime_ns = time.time_ns()
        self.atime_ns = time.time_ns()
        self.ctime_ns = time.time_ns()        
        self.size = 0
        self.rdev = 0
        self.data = [] # Tuple with hash and size
        self.offsets = []
        self.no_compression = False

    

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
        self.DELETION_TIME = None


class FileBlock:
    def __init__(self, _hash, _size, _raw_size):
        self.hash = _hash # Block hash at the hash table
        self.size = _size # Block size with compression
        self.raw_size = _raw_size # Block size without compression
    

class SmallBlock:
    def __init__(self, _hash, _size, _deflated_size, _data):
        self.hash = ""
        self.size = 0  # Tamanho depois da compressao
        self.deflated_size = 0  # Tamanho sem compressao
        self.uses = 1
        self.compressed = False
        self.DELETED = False
        self.DELETION_TIME = str(time.time())
    
        if _size == _deflated_size:
            self.compressed = True


def write_small_block(_hash, data, stat_msg_queue):    
    # env = lmdb.open(small_block_db_path, max_dbs=0, map_size=max_map_size)
    # with env.begin(write=True) as txn:
    #     try:
    #         txn.put(_hash.encode(), data)
    #         txn.commit()            
    #     except:
    #         print(traceback.format_exc())
    #         return 0
    try:
        with SqliteDict(small_block_db_path) as smbs:  # note no autocommit=True
            smbs[_hash] = data
            smbs.commit()
    except:
        print(traceback.format_exc())

    return len(data)


def read_small_block(_hash, stat_msg_queue):    
    # try:
    #     env = lmdb.open(small_block_db_path, max_dbs=0, map_size=max_map_size)
    #     with env.begin() as txn:
    #         with txn.cursor() as curs:
    #             return txn.get(_hash.encode())
    # except:
    #     print(traceback.format_exc())
    #     return False   
    with SqliteDict(small_block_db_path) as smbs:  # note no autocommit=True
        try:
            return smbs[_hash]
        except:
            print(traceback.format_exc())
            return b''
    

def delete_small_block(_hash, stat_msg_queue):
    # try:
    #     env = lmdb.open(small_block_db_path, max_dbs=0, map_size=max_map_size)
    #     with env.begin(write=True) as txn:
    #         txn.delete(_hash.encode())
    #         return True
    # except:
    #     print(traceback.format_exc())
    #     return False
     with SqliteDict(small_block_db_path) as smbs:  # note no autocommit=True
        try:
            del smbs[_hash]
            smbs.commit()
            return True
        except:
            print(traceback.format_exc())
            return False