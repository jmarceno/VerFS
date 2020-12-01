from collections import deque
from collections.abc import MutableSequence
import mmap
from typing import Any
import sys
from utils import load_configuration
import os
import traceback
import pickle

import multiprocessing as mp

confs = load_configuration()

write_ahead_cache_path = confs['write_ahead_cache_path']
write_ahead_cache_initial_size = confs['write_ahead_cache_initial_size']
write_buffer_size = confs['write_buffer_size']

class WriteAhead(MutableSequence):
    def __init__(self, *a, **k):
        # Deque with queud writes on buffer
        self.d = deque()
        # Dict with hash and position on disk of the overflow that has been flushed
        self.on_disk = dict()

        self._next_pos = 0
        #Create cache directoty tree if it does no exist
        if not os.path.isdir(confs['write_ahead_cache_path']):
            os.makedirs(confs['write_ahead_cache_path'])

        # Initialize file to be mmaped
        self.path = os.path.join(str(write_ahead_cache_path),"writeahead.wal" )
        f = open(self.path, "wb")
        f.write(b'INITIALIZATION')
        f.truncate(write_ahead_cache_initial_size)
        f.flush()
        sys.stdout.flush()
        f.close()

        # Open the mmap file and keep it open
        # f = open(self.path, "r+b")        
        # self.mm = mmap.mmap(f.fileno(), length=0, access=mmap.ACCESS_WRITE)

        # TODO: Implment Resume feature to recover data from the WA after a crash
        # if os.path.isdir(hash_table_path):
        #     self.init_from_disk()
        
    def __iter__(self):
        return iter(self.d)

    def __len__(self):
        return len(self.d)
        
    def __getitem__(self, k):    
        return self.d[k]
    
    def popleft(self):
        try:
            return self.d.popleft()
        except KeyError:
            a = next(iter(self.on_disk))
            val = self.read_fom_disk(a)
            del self.on_disk[a]
            return val
        except:
            raise KeyError

    def __contains__(self, k):
        True
        # if k in self.d or k 
        #     return k in self.d

    def __delitem__(self, k):        
        del self.d[k]

    def __setitem__(self, s: slice, o: Any) -> None:        
        if len(self.d) < write_buffer_size:
            self.d[s] = o
        else:
            pos, written = self.save_to_disk(o)
            self.on_disk[pos] =  written

    def insert(self, index: int, value: Any) -> None:
        if len(self.d) < write_buffer_size:
            self.d.append(value)
        else:
            pos, written = self.save_to_disk(value)
            self.on_disk[pos] =  written

    def append(self, value: Any) -> None:
        self.insert(0, value)
        

    '''
    Disk Operations
    '''

    def check_size(self) -> None:
        f = open(self.path, "r+b")
        end = f.seek(0, os.SEEK_END)
        if self._next_pos+(1024*1024) >= end:
            f.truncate(end+(10*(1024**2)))
            f.flush()
            sys.stdout.flush()
        f.close()

        return None

    def save_to_disk(self, cp):
        try:
            written = 0
            pos = 0            
            self.check_size()
            with open(self.path, "r+b") as f:                
                mm = mmap.mmap(f.fileno(), length=0, access=mmap.ACCESS_WRITE)                
                mm.seek(self._next_pos)
                written = mm.write(pickle.dumps(cp))
                self._next_pos = pos = mm.tell()
                del mm
                    
            return pos, written
        
        except:
            print(traceback.format_exc())
            pass

    def read_fom_disk(self, k):
        try:
            self.mm.seek(k)
            return self.mm.read(pickle.loads(self.on_disk[k]))                            
        except:
            print(traceback.format_exc())
            pass

    def remove_from_disk(self, k):                
        try:
            raise NotImplementedError            
        except:
            print(traceback.format_exc())
            return False
    
    '''
    Initialization
    '''
    def init_from_disk(self):        
        try:
            raise NotImplementedError               
            
        except:
            return None
