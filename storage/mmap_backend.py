import mmap
import traceback
import os
import errno
from BTrees import IOBTree
from pyfuse3 import FUSEError

from logger import LogEvent
from datastructures import QueuedWrite, DataStore
from hashtable import HashTable
from compression import decompress_data
from utils import load_configuration

confs = load_configuration()

#TODO: Implement data replication
async def mm_commit(q:QueuedWrite, datastore:DataStore, mirror_datastore:list, free_blocks:IOBTree, fragmentation:dict):
    
    q, datastore, free_blocks, fragmentation  = find_location(q, datastore, free_blocks, fragmentation)
    #TODO: Implement a fail safe route in case of *VALUEERROR* caused by trying to write above the data chunk size limit
    written = 0
    with open(datastore[q['chunk']].path, "r+b") as f:
        mm = mmap.mmap(f.fileno(), length=datastore[q['chunk']].size, access=mmap.ACCESS_WRITE)
        mm.seek(q['block'])
        if q['compressed']:
            written = mm.write(q['compressed_data'])
        else:                            
            written = mm.write(q['data'])        
        mm.close()
        del mm
    
    return written, q, datastore, free_blocks, fragmentation

#TODO: Implement write spread
async def find_location(q:QueuedWrite, datastore:DataStore, free_blocks:IOBTree, fragmentation:dict):
    for idx, ds in enumerate(datastore):
        if not ds.IS_FULL and not ds.LOCKED:
            q['block'] = ds.next_write_position
            if q['compressed']:
                ds.next_write_position = ds.next_write_position + len(q['compressed_data'])
            else:
                ds.next_write_position = ds.next_write_position + len(q['data'])
            q['chunk'] = ds.chunk
            if ds.next_write_position + (1024*1024) > ds.size: # Leave some space at the end of each data chunk to prevent writing above the limit
                ds.IS_FULL = True                                
            break
        elif idx != len(datastore)-1:
            continue
        else:
            for ds, fb in enumerate(free_blocks):
                try:                                    
                    s = fb.minKey(len(q['compressed_data']))
                    q['block'] = fb.get(s).pop(0)
                    q['chunk'] = ds
                    if len(fb.get(s)) == 0:
                        fb.pop(s)
                        fragmentation['free_size'] = fragmentation['free_size'] - s
                        fragmentation['free_count'] = fragmentation['free_count'] - 1                                   
                    break
                except ValueError:
                    if ds == len(free_blocks) - 1:
                        LogEvent(("INFO","Partition FULL. No free blocks that can fit the data."))                                            
                        raise FUSEError(errno.ENOSPC)                                        
                    else:
                        continue   

    return q, datastore, free_blocks, fragmentation   


def mm_read(_hash:str, _block:int, ds:DataStore, read_size:int, hash_table:HashTable):
    try:
        if os.path.isfile(ds.path):
            with open(ds.path, "r+b") as f:
                mm = mmap.mmap(f.fileno(), length=ds.size, access=mmap.ACCESS_READ)
                mm.seek(_block)
                r = mm.read(read_size)            
                mm.close()                                
            d = r[:read_size]                    
            if hash_table[_hash].compressed:
                decompresed_data = decompress_data(d)   # check if the data has been compressed or not. If it was, decompress it, otherwise return data as read
            else:
                decompresed_data = d
            
            d = decompresed_data

            return d
    except:
        return None
