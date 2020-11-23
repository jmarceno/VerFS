from collections.abc import MutableMapping

from sqlitedict import SqliteDict
import rocksdb
from utils import opt

from datastructures import Block
import os
import traceback
import time
import pickle


hash_table_path = os.path.join(os.getcwd(), '..', '..', 'metadata', "hash_table.bin")


class HashTable(MutableMapping):    
    def __init__(self, *a, **k):
        self.d = dict(*a, **k)        
        
        self.ht = rocksdb.DB(hash_table_path, opt())        

        if os.path.isdir(hash_table_path):
            self.init_from_disk()
        
    def __iter__(self):
        return iter(self.d)

    def __len__(self):
        return len(self.d)

    def __getitem__(self, k):        
        return self.d[k]
    
    def __contains__(self, k):
        return k in self.d

    def __delitem__(self, k):
        if self.remove_from_disk(k):
            del self.d[k]

    def __setitem__(self, k, v):
        if self.save_to_disk(k, v):
            self.d[k] = v

    '''
    Disk Operations
    '''

    def save_to_disk(self, k, val):                
        try:
            self.ht.put(k.encode(), pickle.dumps(val))                        
            return True
            
        except:
            print(traceback.format_exc())
            return False

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
