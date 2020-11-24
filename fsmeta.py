import os
from pickle import TRUE
import traceback
import time
import pickle
import rocksdb

from collections.abc import MutableMapping
from datastructures import File_Inode
from utils import opt, load_configuration

confs = load_configuration()

fs_meta_path = confs['fs_meta_path']

class FSMeta(MutableMapping):    
    def __init__(self, *a, **k):
        self.d = dict(*a, **k)
        self.pending = {}
        self.ht = rocksdb.DB(fs_meta_path, opt())
        self.lock = False

        # self.dirs = {}

        if os.path.isdir(fs_meta_path):            
            self.init_from_disk()
        
    def __iter__(self):
        return iter(self.d)

    def __len__(self):
        return len(self.d)

    def __getitem__(self, k):
        return self.d[k]

    def __delitem__(self, k):
        if self.remove_from_disk(k):
            self.dirs[self.d[k].parent_inode].remove(k)
            del self.d[k]
            
    def __setitem__(self, k, v:File_Inode):
        self.d[k] = v
        self.pending[k] = v

    async def commit(self):
        self.lock = True
        
        cp = self.pending.copy()
        self.pending = {}
        
        self.lock = False
        
        self.save_to_disk(cp)

    
    '''
    Disk Operations
    '''

    def save_to_disk(self, cp:dict):
        batch = rocksdb.WriteBatch()
        try:
            for k, v in cp.items():                
                n = (k).to_bytes(64, byteorder='little')
                batch.put(n, pickle.dumps(v))

            self.ht.write(batch)
            return True
        
        except:
            print(traceback.format_exc())
            pass
        

    def remove_from_disk(self, k):
        try:
            n = (k).to_bytes(64, byteorder='little')
            self.ht.delete(n)            
            return True
        except:
            print(traceback.format_exc())
            return False
    
    '''
    Initialization
    '''
    def init_from_disk(self):
        try:
            it = self.ht.iteritems()
            it.seek_to_first()            
            for k, v in it:
                ki = int.from_bytes(k, "little")
                vi = pickle.loads(v)
                vi.lookup_count = 1
                vi.list_on_dir_lookup = True
                self.d[ki] = vi
            
            return self            
        except:
            print(traceback.format_exc())
            return None
