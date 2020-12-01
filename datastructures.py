from collections import deque
from logger import LogEvent
import time
from compression import compress_data
import mmap
import traceback
from utils import datastore_opt
import rocksdb

class Garbage_Collector:
    def __init__(self):
        self.add_uses = deque()  # Hash of blocks that should receive an additional use counter
        self.remove_uses = deque() # Hash of blocks that should have their uses counter decreased


class DataStore:
    def __init__(self, _chunk, _chunk_size, _path, _next_write_position=65, db=None):
        self.chunk = _chunk
        self.size = _chunk_size
        self.path = _path
        self.next_write_position = _next_write_position
        self.LOCKED = False
        self.db = db

        if self.next_write_position + (1024*1024) > self.size:
            self.IS_FULL = True
        else:
            self.IS_FULL = False
    
    def reconnect_db(self):
        self.db = rocksdb.DB(self.path, datastore_opt())

    '''
    Commit the next wwrite position to disk
    Only applies to mmap backend
    '''
    def commit_next_write_position(self):
        with open(self.path, "r+b") as f:
            try:
                mm = mmap.mmap(f.fileno(), length=64, access=mmap.ACCESS_WRITE)
                n = (self.next_write_position).to_bytes(64, byteorder='little')            
                mm.write(n)        
                mm.close()
            except:
                LogEvent(("ERROR", traceback.format_exc()))
                raise IOError
        
        return True



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
        return compress_data(self.data)
        

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
        self.hard_link = 0

    

class Block:
    def __init__(self):
        self.hash = ""
        self.chunk = 0
        self.offset = 0  # Offset dentro do chunk        
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