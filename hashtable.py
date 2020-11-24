from collections.abc import MutableMapping

from sqlitedict import SqliteDict
import rocksdb
from utils import opt, load_configuration

from datastructures import Block
import os
import traceback
import pickle

import multiprocessing as mp

confs = load_configuration()

hash_table_path = confs['hash_table_path']

class HashTable(MutableMapping):    
    def __init__(self, *a, **k):
        self.d = dict(*a, **k)
        self.pending = {}
        
        self.ht = rocksdb.DB(hash_table_path, opt())        

        if os.path.isdir(hash_table_path):
            self.init_from_disk()
        
    def __iter__(self):
        return iter(self.d)

    def __len__(self):
        return len(self.d)

    def __getitem__(self, k):        
        if k in self.d:
            return self.d[k]
        elif k in self.pending[k]:
            return self.pending[k]
        else:
            raise KeyError
    
    def __contains__(self, k):
        return k in self.d

    def __delitem__(self, k):
        if self.remove_from_disk(k):
            del self.d[k]

    def __setitem__(self, k, v):
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
                batch.put(k.encode(), pickle.dumps(v))

            self.ht.write(batch)
            return True
        
        except:
            print(traceback.format_exc())
            pass

    def remove_from_disk(self, k):                
        try:
            self.ht.delete(k.encode())            
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
                self.d[k.decode()] = pickle.loads(v)
                
            return self            
        except:
            return None
