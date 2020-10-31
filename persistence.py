# import debugpy
import os
from compression import decompress_data, decompress_pickle, compress_data, compressed_pickle
from configurations import *
from datastructures import DataStore
import mmap
import copy
import platform

def init_persistance(_fs_meta=None, _keys=None, _datastore=None, _hash=None, _free_blocks=None, _partition_size=None, _GC=None):
    # debugpy.debug_this_thread()
    from configurations import free_blocks_path, key_index_path, datastore_base_path, hash_table_path, gc_path, fs_meta_path
        
    global key_index
    global datastore
    global hash_table
    global free_blocks
    global fs_meta
    global gc_path
    global GC
    global inodes
    global contents
    global fragmentation

    print("Setting-up File System Metadata")
    if os.path.isfile(key_index_path):
        key_index = decompress_pickle(key_index_path)

    if os.path.isfile(gc_path):
        GC = decompress_pickle(gc_path)
    
    if os.path.isfile(hash_table_path):
        hash_table = decompress_pickle(hash_table_path)

    if os.path.isfile(free_blocks_path):
        free_blocks = decompress_pickle(free_blocks_path)
        # print("Recalculating fragmentation:")
        free_blocks_path = _free_blocks
        
        for fb in free_blocks:
            if len(list(fb.keys())) > 0: 
                for b in list(fb.keys()):
                    fragmentation['free_size'] = fragmentation['free_size'] + (len(fb.get(b)) * b)
                    fragmentation['free_count'] = fragmentation['free_count'] + len(fb.get(b))

    if os.path.isfile(fs_meta_path):
        fs_meta = decompress_pickle(fs_meta_path)

    for chunk in datastore_chunks:
        chunk_path = os.path.join(datastore_base_path, 'chunk' + str(chunk) + '.ds.vfs')

        if os.path.isfile(chunk_path):
            # print("Found existing File System. Re-mounting it. Chunk:" +str(chunk))
            datastore.append(DataStore(chunk, chunk_size, chunk_path))
        else:
            f = open(os.path.join(datastore_base_path, 'chunk'+str(chunk)+'.ds.vfs'), "wb")
            if "win" in str(platform.platform()):
                f.write(b'\x00\x00\x00\x00\x00')
            f.truncate(chunk_size)
            f.flush()
            f.close()
            ds = open(chunk_path, "r+b")
            tmp_map = mmap.mmap(ds.fileno(), length=chunk_size, access=mmap.ACCESS_WRITE)
            datastore.append(DataStore(chunk, chunk_size, chunk_path))
            tmp_map.close()
            ds.close()

            free_blocks.append(IOBTree.IOBTree())

    print("File System Ready")
    return datastore, free_blocks, key_index, hash_table, fs_meta, GC


def persist_data(fs=None, stat_msg_queue=None):
    # debugpy.debug_this_thread()
    global key_index
    global datastore
    # global fs_meta
    global key_index_path
    global fs_meta_path
    global datastore_path
    global write_buffer    
    global hash_table
    global hash_table_path
    global free_blocks_path
    global lock
    global GC
    global gc_path
    
    if not compressed_pickle(key_index_path, key_index.copy()):
        stat_msg_queue.put("DEBUG: Persistence of Key Index Deferred.")
    
    if not compressed_pickle(hash_table_path, hash_table.copy()):
        stat_msg_queue.put("DEBUG: Persistence of Hash Table Deferred.")
    
    if not compressed_pickle(free_blocks_path, free_blocks.copy()):
        stat_msg_queue.put("DEBUG: Persistence of Free Blocks Deferred.")
    
    if not compressed_pickle(gc_path, copy.copy(GC)):
        stat_msg_queue.put("DEBUG: Persistence of GC Deferred.")
    
    if not compressed_pickle(fs_meta_path, fs):
        stat_msg_queue.put("DEBUG: Persistence File System Meta Info Deferred.")

    return True