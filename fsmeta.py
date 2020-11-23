import os
import traceback
import time
import pickle
import rocksdb

from collections.abc import MutableMapping
from datastructures import File_Inode
from utils import opt


fs_meta_path = os.path.join(os.getcwd(), '..', '..', 'metadata', "fs.meta")


class FSMeta(MutableMapping):    
    def __init__(self, *a, **k):
        self.d = dict(*a, **k)

        self.ht = rocksdb.DB(fs_meta_path, opt())
        
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
            del self.d[k]

    def __setitem__(self, k, v:File_Inode):
        if self.save_to_disk(k, v):
            self.d[k] = v

    
    '''
    Disk Operations
    '''

    def save_to_disk(self, k, val:File_Inode):
        try:            
            n = (k).to_bytes(64, byteorder='little')
            self.ht.put(n, pickle.dumps(val))
            return True
        except:
            print(traceback.format_exc())
            return False
        

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
                self.d[int.from_bytes(k, "little")] = pickle.loads(v)
            
            return self            
        except:
            return None
