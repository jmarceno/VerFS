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
import collections

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

# __getattr__
# def __getattribute__(self, name):
#     print('Attribute error. Is this a Hard Link? =', name)
#     if name == 'foo':
#         return 0
#     else:
#         return super().__getattribute__(name)


class FileSystem(collections.MutableMapping):
    def __init__(self):
        self.nodes = {}
        self.pointers = {}

    def get_dir(self, parent_inode, offset=0):
        d = []
        for x, p in enumerate(self.pointers, offset):
            if self.pointers[p].parent_inode == parent_inode:
                d.append(self.get_node(p))
        return d

    def get_node(self, inode):
        if not self.pointers[inode].hard_link:
            return Full_Node(self.inodes[inode], self.pointers[inode])
        else:
            return Full_Node(self.inodes[self.pointers[inode].inode], self.pointers[inode])
            

    def add_node(self, name, parent_inode, inode=0, hard_link=False, sym_target=""):
        if not hard_link:
            inode = self.gen_inode_number()
            self.nodes[inode] = File_Inode(inode=inode)
            self.pointers[inode] = Pointers(inode, name, parent_inode, sym_target)
        else:
            new_inode = self.gen_inode_number()
            self.nodes[inode].st_nlink += 1
            self.pointers[new_inode] = Pointers(inode, name, parent_inode, True, sym_target)


    def remove_node(self, inode):
        if not self.pointers[inode].hard_link:
            del self.pointers[inode]
            del self.nodes[inode]
        else:
            self.nodes[self.pointers[inode].inode].st_nlink -= 1
            del self.pointers[inode]


    def gen_inode_number(self):
        return max(self.nodes) + 1


class Full_Node(object):
    def __init__(self, pointer, inode):
        self.inode = pointer.inode
        self.name = pointer.name
        self.sym_target = pointer.sym_target
        self.parent_inode = pointer.parent_inode
        self.hard_link = pointer.hard_link
        self.list_on_dir_lookup = pointer.list_on_dir_lookup     
        self.st_nlink = inode.st_nlink
        self.lookup_count = inode.lookup_count
        self.uid = inode.uid
        self.gid = inode.gid
        self.mode = inode.mode
        self.mtime_ns = inode.mtime_ns
        self.atime_ns = inode.atime_ns
        self.ctime_ns = inode.ctime_ns        
        self.size = inode.size
        self.rdev = inode.rdev
        self.data = inode.data
        self.offsets = inode.offsets


class Pointers(object):
    def __init__(self, inode, name, parent_inode, hard_link=False, sym_target=""):
        self.inode = inode # The actual inode, or the inode that it points to
        self.name = name
        self.sym_target = ""
        self.parent_inode = parent_inode
        self.hard_link = hard_link
        self.list_on_dir_lookup = True


class File_Inode(object):
    def __init__(self, inode=-1):
        # self.inode = inode
        #self.parent_inode = 0
        # self.name = ""        
        # self.target = ""
        #self.target_inode = -1
        self.st_nlink = 1
        # self.list_on_dir_lookup = True
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