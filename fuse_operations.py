# import debugpy
# debugpy.debug_this_thread()

import os.path
import time
import copy
import mmap
import hashlib
import traceback
import lzma
import math
import struct
from hashing import hashed_chunks, hash_data
from stats import Timer, humanbytes, memory
from compression import compressed_pickle, decompress_pickle, decompress_data, compress_data
from BTrees import IOBTree
from collections import OrderedDict, deque

import sys
import logging
from functools import wraps, lru_cache

from configurations import *
from datastructures import *


def get_small_blocks_real_size(start_path = '.'):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            # skip if it is symbolic link
            if not os.path.islink(fp):
                total_size += os.path.getsize(fp)

    return total_size

def get_usage():
    """
    TODO: RECONSIDERAR TROCAR O KEY-INDEX POR UTLIZAÇÃO DE INDICE COM QUANTIDADE NOS BLOCOS
    Return drive virtual (undeduped size) and physical (deduped_size) utilization
    :return: Undeduped Data Un-Compressed, Undeduped Data Compressed, Deduped Data Compressed, Deduped Data Removed, Compression rate
    """
    # debugpy.debug_this_thread()
    global hash_table
    global key_index

    undeduped_compressed = 0
    undeduped_uncompressed = 0
    deduped_compressed = 0
    compression_rate = 0.0

    key_index_copy = key_index.copy()
    for k in key_index_copy:        
        try:
            if not hash_table[k].DELETED:
                undeduped_uncompressed = undeduped_uncompressed + (key_index_copy[k] * hash_table[k].deflated_size)
                undeduped_compressed = undeduped_compressed + (key_index_copy[k] * hash_table[k].size)
                deduped_compressed = deduped_compressed + hash_table[k].size
        except KeyError:
            continue

    if undeduped_compressed != 0 and undeduped_uncompressed != 0:
        compression_rate = undeduped_compressed/undeduped_uncompressed

    print("Undeduped (No Compression) Space Used : " + humanbytes(undeduped_uncompressed))
    print("Undeduped (Compression) Space Used : " + humanbytes(undeduped_compressed))
    print("Deduped (Compression) Space Used : " + humanbytes(deduped_compressed) + " [Duplicated data found (Saved Space): " + humanbytes(undeduped_uncompressed -deduped_compressed) + " ]")
    print("Compression Rate : " + str(1 - compression_rate) + "% Saved Space: " + humanbytes(undeduped_uncompressed - undeduped_compressed))
    print("Total Savings: " + humanbytes((undeduped_uncompressed - undeduped_compressed)+(undeduped_uncompressed -deduped_compressed)))

    del key_index_copy

    return undeduped_uncompressed, undeduped_compressed, deduped_compressed, (undeduped_compressed-deduped_compressed), compression_rate


def garbage_collector():
    # debugpy.debug_this_thread()
    global free_blocks
    global hash_table
    global key_index


    try:        
        while len(GC.add_uses) > 0:
            add = GC.add_uses.popleft()
            hash_table[add].uses = hash_table[add].uses + 1            
    except IndexError:
        pass

    try:        
        while len(GC.remove_uses) > 0:
            remove = GC.remove_uses.popleft()
            hash_table[remove].uses = hash_table[remove].uses - 1
            if hash_table[remove].uses <= 0:
                if hash_table[remove].size <= small_block_limit:
                    r = delete_small_block(remove)
                    if not r:
                        print("DEBUG: Block could not be deleted. File "+ str(remove) +" is now orphan. Please manually delete.")
                hash_table[remove].DELETED = True
                try:
                    free_blocks[hash_table[remove].chunk].get(hash_table[remove].size).append(hash_table[remove].offset)
                except AttributeError:
                    free_blocks[hash_table[remove].chunk].insert(hash_table[remove].size, [hash_table[remove].offset])
                
    except IndexError:
        pass


# Checa se a posiçao do datastore já existia no dicionario de indice
# Caso exista, adiciona uma referencia
# Caso não exista, cria nova entrada no dicionário e coloca a quantidade de referncias como 1
# A quantidade de referencias indica quantas vezes aquele bloco esta sendo usado, quanto chegar a zero, ele deve ser
# removido ou sobrescrito

def update_index(idx, chunk=None, add=True):
    """

    :param chunk:
    :param idx: Hash of the block as of in the hash_table
    :param add: Operation. Should the block usage count go up or down?
    :return:
    """
    # debugpy.debug_this_thread()
    global key_index
    global lock
    in_index = False

    
    if idx in key_index:
        in_index = True

    if add:
        if in_index:
            key_index[idx] = int(key_index[idx] + 1)
            hash_table[idx].uses = hash_table[idx].uses + 1
        else:
            key_index[idx] = 1
    else:
        try:
            if in_index and key_index[idx] - key_index[idx] <= 0:
                # free_blocks[chunk].append(hash_table[idx])
                GC.remove_uses.append(idx)
                hash_table[idx].DELETED = True
                hash_table[idx].DELETION_TIME = time.time()
                try:
                    del key_index[idx]
                    del read_cache[idx]
                except KeyError:
                    pass
                # update_hash_table(_hash=idx, _operation=-1)
            elif in_index:
                key_index[idx] = key_index[idx] - 1
                hash_table[idx].uses = hash_table[idx].uses - -1
        except Exception:
            print(traceback.format_exc())
            raise IOError

    return True


def write_new_blocks(_queued_writes):
    """
    Writes a series of blocks that where quede

    :param _queued_writes: data to be written
    :return: Tuple with the result of the operation and position (block) that the data has been written to
    """
    # debugpy.debug_this_thread()
    global chunk_size
    global datastore
    global free_blocks
    global write_buffer_lock
    global hash_table
    global fs_meta

    registers_processed = 0
    bytes_processed = 0
    writing = True
    start_time = time.time()

    write_buffer_lock = True
    while writing:

        try:
            q = write_buffer.popleft()
            if not q.result:
                try:
                    written = 0
                    written_hash = ""

                    if len(q.compressed_data) <= small_block_limit:
                        if q.compressed:
                            written = len(q.compressed_data)
                            write_small_block(q.hash, q.compressed_data)
                            written_hash = hash_data(q.compressed_data)
                            q.chunk = -1                            
                        else:
                            written = len(q.data)
                            write_small_block(q.hash, q.data)
                            written_hash = hash_data(q.data)
                            q.chunk = -1                        
                    else:
                        for idx, ds in enumerate(datastore):
                            if not ds.IS_FULL:
                                q.block = ds.next_write_position
                                if q.compressed:
                                    ds.next_write_position = ds.next_write_position + len(q.compressed_data)
                                else:
                                    ds.next_write_position = ds.next_write_position + len(q.data)
                                q.chunk = ds.chunk
                                if ds.next_write_position + max_blk_size > ds.size:
                                    ds.IS_FULL = True
                                break
                            elif idx != len(datastore)-1:
                                continue
                            else:
                                for ds, fb in enumerate(free_blocks):
                                    try:                                    
                                        s = fb.minKey(len(q.compressed_data))
                                        q.block = fb.get(s).pop(0)
                                        if len(fb.get(s)) == 0:
                                            fb.pop(s)
                                        # free_blocks[hash_table[remove].chunk][0].pop(hash_table[remove].offset)
                                        break
                                    except ValueError:
                                        if ds == len(free_blocks) - 1:
                                            print("Partition FULL. No free blocks that can fit the data.")
                                            raise IOError
                                        else:
                                            continue                    
                        try:
                            if os.path.isfile(datastore[q.chunk].path):     # TODO: Organize all the data in one single write
                                with open(datastore[q.chunk].path, "r+b") as f:
                                    mm = mmap.mmap(f.fileno(), length=datastore[q.chunk].size, access=mmap.ACCESS_WRITE)
                                    mm.seek(q.block)
                                    if q.compressed:
                                        written = mm.write(q.compressed_data)
                                        written_hash = hash_data(q.compressed_data)
                                    else:
                                        written = mm.write(q.data)
                                        written_hash = hash_data(q.data)
                        except ValueError:
                            print("ValueError Writing data to the disk: Chunk:{}, Block:{}, Data Size:{}".format(q.chunk, q.block, len(q.compressed_data)))
                            print(traceback.format_exc())

                    if not q.compressed and written_hash != q.hash:
                        print("Data corruption - Hash inconsistance")

                    if q.compressed and written_hash != hash_data(q.compressed_data):
                        print("Data corruption - Hash inconsistance")

                    if len(q.compressed_data) == written or len(q.data) == written:
                        q.result = True
                        # update_hash_table(q.hash, q.block, q.chunk, 1)
                        hash_table[q.hash] = Block()
                        hash_table[q.hash].hash = q.hash
                        hash_table[q.hash].chunk = q.chunk
                        hash_table[q.hash].offset = q.block
                        hash_table[q.hash].size = len(q.compressed_data)
                        hash_table[q.hash].deflated_size = len(q.data)
                        hash_table[q.hash].compressed = q.compressed

                        if len(q.compressed_data) <= small_block_limit:
                            small_block_read_cache[q.hash] = q.data

                        update_index(q.hash, q.chunk, True)

                        registers_processed = registers_processed + 1
                        bytes_processed = bytes_processed + written
                        try:
                            del write_read_cache[q.hash]
                        except KeyError:
                            print('DESGRAÇA')
                        continue
                    else:
                        print("IOError Writing data to the disk: Chunk:{}, Block:{}, Data Size:{}".format(q.chunk, q.block, len(q.compressed_data)))
                        print(traceback.format_exc())
                        raise IOError

                except Exception:
                    print(traceback.format_exc())

        except IndexError:
            if registers_processed > 0:
                print("DEBUG: Write Queue has been processed. " + str(registers_processed) + " registers")
                print("DEBUG: Processed -> "+humanbytes(bytes_processed)+" in "+humanbytes(bytes_processed/(time.time()-start_time))+" /s")
                print("TODO: IMPLEMENT METADATA PERSISTANCE")
                # persist_data(fs_meta)
            writing = False
            write_buffer_lock = False
            break
    write_buffer_lock = False
    # return _queued_writes


def dedup(data):
    # debugpy.debug_this_thread()
    global datastore
    global free_blocks    
    global hash_table
    global lock
    global read_cache
    global write_buffer
    global write_read_cache

    blk_list = []
    queued_writes = []

    if type(data) == bytearray or type(data) == bytes:
        if type(data) == bytes:
            data = bytearray(data)

        ch = variable_chunks(data)
        for c in ch:
            # read_cache[c.hash] = c.data
            blk_list.append(0)

            if c.hash in hash_table:
                blk_list[len(blk_list)-1] = FileBlock(c.hash, len(c.data))
                update_index(c.hash)
            else:
                done = False
                for q in queued_writes:   #  Verify if this block has already been processed in this batch
                    if q.hash == c.hash:
                        blk_list[len(blk_list)-1] = FileBlock(c.hash, len(c.data))
                        GC.add_uses.append(c.hash)                        
                        done = True                        
                        break
                wb = write_buffer.copy()
                for w in wb:   #  Verify if this block is already in the write queue to be writtn
                    if w.hash == c.hash:
                        blk_list[len(blk_list)-1] = FileBlock(c.hash, len(c.data))
                        GC.add_uses.append(c.hash)                        
                        done = True
                        break
                del wb
                if not done:
                    q = QueuedWrite(len(blk_list)-1, c.hash, c.data)
                    blk_list[q.idx] = FileBlock(q.hash, len(c.data))
                    write_read_cache[c.hash] = c.data
                    write_buffer.append(q)

    else:
        print("Value Error when preparing writes")
        print(traceback.format_exc())
        raise ValueError

    return blk_list


def get_file_data(blklst, start_block=None, end_block=None, offset=0, end_offset=0, check_integrity=False):
    # debugpy.debug_this_thread()
    global datastore
    global allocation_unit
    global hash_table
    global read_cache

    data = bytearray()

    cached = False    
    r = bytearray()

    for idx, b in enumerate(blklst):
        if idx < start_block:
            continue
            # at_start = at_start + 1
        elif idx > end_block:
            return data

        if idx >= start_block:
            d = seek_in_cache(b.hash)
            if d is not None:
                if len(d) != b.size:
                    print("DEBUG: Invalid data on cache entry")
                    d = None
                    del read_cache[b.hash]
            if d is None and len(r) > b.size:
                if hash_data(r[:hash_table[b.hash].size]) == b.hash:
                    d = r[:b.size]
                    r = r[b.size:]
                    read_cache[b.hash] = d
            
            if hash_table[b.hash].chunk == -1:
                try:
                    d = read_small_block(b.hash)
                    if hash_table[b.hash].compressed:
                            d = decompress_data(d)   # check if the data has been compressed or not. If it was, decompress it, otherwise return data as read                    
                    small_block_read_cache[b.hash] = d
                except Exception:
                    print(traceback.format_exc())
                    pass

            if d is None:
                try:
                    _chunk = hash_table[b.hash].chunk
                    _block = hash_table[b.hash].offset
                    _hash = hash_table[b.hash].hash
                    read_size = hash_table[b.hash].size
                except KeyError:
                    time.sleep(0.01)
                    d = seek_in_cache(b.hash)
                    if d is not None:
                        break

                    _chunk = hash_table[b.hash].chunk
                    _block = hash_table[b.hash].offset
                    _hash = hash_table[b.hash].hash
                    read_size = hash_table[b.hash].size

                if os.path.isfile(datastore[_chunk].path):
                    with open(datastore[_chunk].path, "r+b", buffering=over_read_limit) as f:
                        mm = mmap.mmap(f.fileno(), length=chunk_size, access=mmap.ACCESS_WRITE)
                        mm.seek(_block)
                        r = mm.read(read_size + over_read_limit)
                        d = r[:read_size]
                        r = r[read_size:]

                        if hash_table[b.hash].compressed:
                            decompresed_data = decompress_data(d)   # check if the data has been compressed or not. If it was, decompress it, otherwise return data as read
                        else:
                            decompresed_data = d

                        read_cache[_hash] = decompresed_data

                    # if _hash != hash_data(decompresed_data):
                    #     print('Critical data failure')
                        # decompresed_data = d
                    d = decompresed_data

                else:
                    print("IOError when trying to read physical media")
                    print(traceback.format_exc())
                    raise IOError

            if b.hash != hash_data(d):
                print('Critical data failure')
            if d is not None:
                data += d

    return data


def seek_in_cache(_blk_hash):
    try:
        return read_cache[_blk_hash]
    except KeyError:
        try:
            return write_read_cache[_blk_hash]
        except KeyError:
            try:
                return small_block_read_cache[_blk_hash]
            except KeyError:
                return None


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def variable_chunks(data):
    return hashed_chunks(data)
