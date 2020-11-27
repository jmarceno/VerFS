import mmap
import traceback
import os
import errno
from pyfuse3 import FUSEError
from BTrees import IOBTree
from logger import LogEvent
from utils import load_configuration
from datastructures import DataStore, Block, QueuedWrite
from hashtable import HashTable
from storage.mmap_backend import mm_commit, mm_read
from storage.rocksdb_backend import rocksdb_commit, rocksdb_read

confs = load_configuration()
mean_blk_size = confs['mean_blk_size']
backend = confs['backend']


async def commit_data(q:QueuedWrite, datastore:DataStore, free_blocks:IOBTree, fragmentation:dict):
    if backend == 'mmap' or backend == None:
        written, q, datastore, free_blocks, fragmentation = await mm_commit(q, datastore, free_blocks, fragmentation)

        return written, q, datastore, free_blocks, fragmentation
    
    elif backend == 'rocksdb':
        written, q, datastore, free_blocks, fragmentation = await rocksdb_commit(q, datastore, free_blocks, fragmentation)

        return written, q, datastore, free_blocks, fragmentation


def read_data(hash:str, offset:int, ds:DataStore, read_size:int, hashtable:HashTable):
    if backend == 'mmap' or backend == None:
        return mm_read(hash, offset, ds, read_size, hashtable)
    elif backend == 'rocksdb':
        return rocksdb_read(hash, offset, ds, read_size, hashtable)
