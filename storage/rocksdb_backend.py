import rocksdb
import traceback
import os
import errno
import random
from BTrees import IOBTree
from pyfuse3 import FUSEError
import trio

from logger import LogEvent
from datastructures import QueuedWrite, DataStore
from hashtable import HashTable
from compression import decompress_data
from utils import load_configuration, datastore_opt, get_dir_size, opt

confs = load_configuration()
write_spread = confs['write_spread']
replication_type = confs['replication_type'].lower()
replication = confs['replicate_data']

#TODO: Change writes to be done in batches. How to do that for various databases

async def rocksdb_batch_commit(q:dict, datastore:list, free_blocks:IOBTree, fragmentation:dict):
    raise NotImplementedError
    # batch = rocksdb.WriteBatch()
    # try:
        
    #     datastore.write(batch)
    # return True

'''
Receives a list of datastores and return one suitable for writing, trying to spread the writes
'''
def pick_datastore(datastore:list) -> int:
    l = []
    [l.append(x) for x in range(len(datastore))]    
    
    while len(l) > 0:
        if write_spread:
            store = random.choice(l)
        else:
            store = l[0]

        datastore[store].next_write_position = get_dir_size(datastore[store].path)
        if datastore[store].next_write_position < datastore[store].size:
            return store
        else:
            l.remove(store)
    
    return -1
    

def write_data(q:dict, datastore:list, data:memoryview, replica:bool=False) -> bool:
    store = pick_datastore(datastore)
    if store != -1:
        try:
            if not replica:
                q['chunk'] = datastore[store].chunk        
                q['block'] = datastore[store].next_write_position
            datastore[store].db.put(q['hash'].encode(), bytes(data))
            return 1
        except:
            raise IOError
    else:
        raise FUSEError(errno.ENOSPC)


def rocksdb_commit(q:QueuedWrite, datastore:list, mirror_datastore:list, free_blocks:IOBTree, fragmentation:dict):
    written = 0

    if q['compressed']:
        data = q['compressed_data']
        written = len(q['compressed_data'])
    else:
        data = q['data']
        written = len(q['data'])     
    
    write_data(q, datastore, data)

    if replication:        
        write_data(q, mirror_datastore, data, replica=True)        
    
    return written, q, datastore, mirror_datastore, free_blocks, fragmentation


def rocksdb_read(_hash:str, _block:int, datastore:DataStore, read_size:int, hash_table:HashTable):   
    try:
        data = datastore.db.get(_hash.encode())
        
        if hash_table[_hash].compressed:
            return decompress_data(data)
                # check if the data has been compressed or not. If it was, decompress it, otherwise return data as read
        else:
            return data        
    except:
        LogEvent(("ERROR",traceback.format_exc()))
        return bytes(0)


def rocksdb_delete(_hash:str, datastore:str):
    # db = rocksdb.DB(datastore.path, datastore_opt())
    try:
        datastore.db.delete(_hash.encode())
    except:
        LogEvent(("ERROR",traceback.format_exc()))
        