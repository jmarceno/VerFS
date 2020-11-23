# import debugpy
import os
from compression import decompress_data, decompress_pickle, compress_data, compressed_pickle
from configurations import *
from datastructures import DataStore
import mmap
import copy
import platform

def init_persistance( _datastore=None, _free_blocks=None, _partition_size=None, _GC=None):    
    from configurations import free_blocks_path, datastore_base_path, gc_path
    
    global datastore    
    global free_blocks
    global GC
    global fragmentation

    print("Setting-up File System Metadata")
    
    if os.path.isfile(gc_path):
        GC = decompress_pickle(gc_path)
    
    if os.path.isfile(free_blocks_path):
        free_blocks = decompress_pickle(free_blocks_path)
        print("Recalculating fragmentation:")
        free_blocks_path = _free_blocks
        
        for fb in free_blocks:
            if len(list(fb.keys())) > 0: 
                for b in list(fb.keys()):
                    fragmentation['free_size'] = fragmentation['free_size'] + (len(fb.get(b)) * b)
                    fragmentation['free_count'] = fragmentation['free_count'] + len(fb.get(b))
    
    chunk_msg = False
    for chunk in datastore_chunks:        
        chunk_path = os.path.join(datastore_base_path, 'chunk' + str(chunk) + '.ds.vfs')

        if os.path.isfile(chunk_path):
            if not chunk_msg:
                print("Found existing File System. Re-mounting "+ str(chunk) + " Chunks.")
                chunk_msg = True
            
            with open(chunk_path, "r+b") as f:
                mm = mmap.mmap(f.fileno(), length=chunk_size, access=mmap.ACCESS_WRITE)        
                mm.seek(0)
                # d = pickle.loads(mm.read(64))
                d = int.from_bytes(mm.read(64), "little")
                datastore.append(DataStore(chunk, chunk_size, chunk_path, d))
                mm.close()
                del mm

        else:
            f = open(os.path.join(datastore_base_path, 'chunk'+str(chunk)+'.ds.vfs'), "wb")            
            n = (65).to_bytes(64, byteorder='little')
            f.write(n)
            f.truncate(chunk_size)
            f.flush()
            f.close()
            datastore.append(DataStore(chunk, chunk_size, chunk_path))            

            if not os.path.isfile(free_blocks_path):
                free_blocks.append(IOBTree.IOBTree())

    print("File System Ready")

    return datastore, free_blocks, GC


def persist_data(stat_msg_queue=None):
        
    global free_blocks_path    
    global GC
    global gc_path
        
    if not compressed_pickle(free_blocks_path, free_blocks.copy()):
        stat_msg_queue.put("DEBUG: Persistence of Free Blocks Deferred.")
    
    if not compressed_pickle(gc_path, copy.copy(GC)):
        stat_msg_queue.put("DEBUG: Persistence of GC Deferred.")
    
    return True