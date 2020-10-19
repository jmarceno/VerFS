# import debugpy
import os
from compression import decompress_data, decompress_pickle, compress_data, compressed_pickle
from configurations import *
from datastructures import DataStore
import mmap
import copy

def init_persistance(_fs_meta=None, _keys=None, _datastore=None, _hash=None, _free_blocks=None, _partition_size=None, _GC=None):
    # debugpy.debug_this_thread()
    global key_index
    global datastore
    global hash_table
    global free_blocks
    global fs_meta
    global key_index_path
    global fs_meta_path
    global datastore_path
    global hash_table_path
    global free_blocks_path
    global gc_path
    global GC
    global inodes
    global contents

    if _keys is None:
        key_index_path = os.path.join(os.getcwd(), 'metadata', "key_index.vfs")
    else:
        key_index_path = _keys

    if os.path.isfile(key_index_path):
        key_index = decompress_pickle(key_index_path)

    if _GC is None:
        gc_path = os.path.join(os.getcwd(), 'metadata', "gc.vfs")
    else:
        gc_path = _GC

    if os.path.isfile(gc_path):
        GC = decompress_pickle(gc_path)

    if _hash is None:
        hash_table_path = os.path.join(os.getcwd(), 'metadata', "hash_table.bin")
    else:
        hash_table_path = _hash

    if os.path.isfile(hash_table_path):
        hash_table = decompress_pickle(hash_table_path)

    if _free_blocks is None:
        free_blocks_path = os.path.join(os.getcwd(), 'metadata', "free_blocks.bin")
    else:
        free_blocks_path = _free_blocks

    if os.path.isfile(free_blocks_path):
        free_blocks = decompress_pickle(free_blocks_path)

    if _fs_meta is None:
        fs_meta_path = os.path.join(os.getcwd(), 'metadata', "fs.meta")
    else:
        fs_meta_path = _fs_meta

    if os.path.isfile(fs_meta_path):
        fs_meta = decompress_pickle(fs_meta_path)
        inodes = fs_meta[0]
        contents = fs_meta[1]

    if _datastore is None:
        datastore_path = os.path.join(os.getcwd(), 'metadata')
    else:
        datastore_path = _datastore

    for chunk in datastore_chunks:
        chunk_path = os.path.join(os.getcwd(), 'metadata', 'chunk' + str(chunk) + '.ds.vfs')

        if os.path.isfile(chunk_path):
            print("Found existing File System. Re-mounting it. Chunk:" +str(chunk))
            datastore.append(DataStore(chunk, chunk_size, chunk_path))
        else:
            f = open(os.path.join(os.getcwd(), 'metadata', 'chunk'+str(chunk)+'.ds.vfs'), "wb")
            # f.write(b'\x00\x00\x00\x00\x00')
            f.truncate(chunk_size)
            f.flush()
            f.close()
            ds = open(chunk_path, "r+b")
            tmp_map = mmap.mmap(ds.fileno(), length=chunk_size, access=mmap.ACCESS_WRITE)
            datastore.append(DataStore(chunk, chunk_size, chunk_path))
            tmp_map.close()
            ds.close()

            free_blocks.append(IOBTree.IOBTree())

    return datastore, free_blocks, key_index, hash_table, fs_meta


def persist_data(fs=None):
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
        print("DEBUG: Persistence of Key Index Deferred.")
    
    if not compressed_pickle(hash_table_path, hash_table.copy()):
        print("DEBUG: Persistence of Hash Table Deferred.")
    
    if not compressed_pickle(free_blocks_path, free_blocks.copy()):
        print("DEBUG: Persistence of Free Blocks Deferred.")
    
    if not compressed_pickle(gc_path, copy.copy(GC)):
        print("DEBUG: Persistence of GC Deferred.")
    
    if not compressed_pickle(fs_meta_path, fs):
        print("DEBUG: Persistence File System Meta Info Deferred.")

    return True



