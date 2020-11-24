import os
from pickle import TRUE
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
        # while self.lock:
        #     time.sleep(0)

        self.d[k] = v
        self.pending[k] = v
        
        # if v.parent_inode in self.dirs:
        #     self.dirs[v.parent_inode].append(k)
        # else:
        #     self.dirs[v.parent_inode] = [k]

    # def get_dir(self, k:int, off=0):
    #     dirs = []
    #     for i in self.dirs[k]:
    #         if self.d[i].list_on_dir_lookup:
    #             dirs.append(self.d[i])
        
    #     return dirs[off:]

    async def commit(self):
        self.lock = True
        
        cp = self.pending.copy()
        self.pending = {}
        
        self.lock = False
        
        for k, v in cp.items():
            try:                
                self.save_to_disk(k, v)
            except:
                print(traceback.format_exc())
                pass
        del cp
            
            
    
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
                ki = int.from_bytes(k, "little")
                vi = pickle.loads(v)
                vi.lookup_count = 1
                vi.list_on_dir_lookup = True
                self.d[ki] = vi

                # if vi.parent_inode in self.dirs:
                #     self.dirs[vi.parent_inode].append(ki)
                # else:
                #     self.dirs[vi.parent_inode] = [ki]
            
            return self            
        except:
            print(traceback.format_exc())
            return None
