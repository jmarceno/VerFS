from collections import deque
from collections.abc import MutableMapping
import mmap

from utils import load_configuration
import os
import traceback
import pickle

import multiprocessing as mp

confs = load_configuration()

write_ahead_cache_path = confs['write_ahead_cache_path']
write_ahead_cache_initial_size = confs['write_ahead_cache_initial_size']
write_buffer_size = confs['write_buffer_size']

class WriteAhead(MutableMapping):    
    def __init__(self, *a, **k):
        # Deque with queud writes on buffer
        self.d = deque()
        # Dict with hash and position on disk of the overflow that has been flushed
        self.on_disk = deque()

        self._next_pos = 0
        #Create cache directoty tree if it does no exist
        if not os.path.isdir(confs['write_ahead_cache_path']):
            os.makedirs(confs['write_ahead_cache_path'])

        # Initialize file to be mmaped
        f = open(write_ahead_cache_path+'.wal', "wb")
        n = bytes(15)
        f.write(n)
        f.truncate(write_ahead_cache_initial_size)
        f.flush()
        f.close()

        # Open the mmap file and keep it open
        f = open(write_ahead_cache_path+'.wal', "wb")        
        self.mm = mmap.mmap(f.fileno(), length=write_ahead_cache_initial_size, access=mmap.ACCESS_WRITE)

        # TODO: Implment Resume feature to recover data from the WA after a crash
        # if os.path.isdir(hash_table_path):
        #     self.init_from_disk()
        
    def __iter__(self):
        return iter(self.d)

    def __len__(self):
        return len(self.d)
    
    def __getitem__(self, k):        
        # Implement
        if k in self.d:
            return self.d[k]        
        else:
            raise KeyError
    
    def __contains__(self, k):
        return k in self.d

    def __delitem__(self, k):
        # TODO: Implement
        if self.remove_from_disk(k):
            del self.d[k]

    def __setitem__(self, k):
        if len(self.d) < write_buffer_size:
            self.d.append(k)
        else:
            pos, written = self.save_to_disk(k)
            self.on_disk.append((pos, written))

        
    '''
    Disk Operations
    '''

    def save_to_disk(self, cp):
        try:
            self.mm.seek(self._next_pos)
            pos = self.mm.tell()
            written = self.mm.write(pickle.dumps(cp))
            self._next_pos = self._next_pos + written
        
            return pos, written
        
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
