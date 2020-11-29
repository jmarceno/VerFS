from logger import LogEvent
import os
import traceback
import pickle
import rocksdb
import copy

from collections.abc import MutableMapping
from datastructures import File_Inode
from utils import opt, load_configuration

confs = load_configuration()

fs_meta_path = confs['fs_meta_path']

class FSMeta(MutableMapping):    
    def __init__(self, *a, **k):
        self.d = dict(*a, **k)
        self.pending = {}

        #Create FSMeta directoty tree if it does no exist
        if not os.path.isdir(confs['fs_meta_path']):
            os.makedirs(confs['fs_meta_path'])

        self.ht = rocksdb.DB(fs_meta_path, opt())

        self.replication = confs['replicate_metadata']
        if self.replication:
            self.replication_path = confs['fsmeta_replication_path']
            if not os.path.isdir(self.replication_path):
                os.makedirs(self.replication_path)
            self.mirror = rocksdb.DB(self.replication_path, opt())
        
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
            if k in self.pending:
                del self.pending[k]
            del self.d[k]
        else:
            t = traceback.format_exc()
            LogEvent(("Error",t))
            print("not removed from disk")            
            
    def __setitem__(self, k, v:File_Inode):
        # if self.save_to_disk(k, v):
        self.d[k] = v
        self.pending[k] = v

    async def commit(self):        
        cp = self.pending.copy()
        self.pending = {}              
        self.batch_save_to_disk(cp)
        del cp

    def decrease_st_nlink(self, k):
        self.d[k].st_nlink = self.d[k].st_nlink - 1
        # self.pending[k] = self.d[k]

    def max(self):
        return max(self.d)
    
    '''
    Disk Operations
    '''

    def save_to_disk(self, k:int, v:File_Inode):
        try:
            n = (k).to_bytes(64, byteorder='little')
            self.ht.put(n, pickle.dumps(v))
            return True
        except:
            LogEvent(("ERROR",traceback.format_exc()))
            print(traceback.format_exc())


    def batch_save_to_disk(self, cp:dict):
        batch = rocksdb.WriteBatch()
        try:
            for k, v in cp.items():                
                n = (k).to_bytes(64, byteorder='little')
                batch.put(n, pickle.dumps(v))                
            self.ht.write(batch)
            
            if self.replication:
                self.mirror.write(batch)
            
            return True
        
        except:
            print(traceback.format_exc())
            pass
        

    def remove_from_disk(self, k):
        try:
            n = (k).to_bytes(64, byteorder='little')
            self.ht.delete(n)

            if self.replication:
                self.mirror.delete(n)

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
