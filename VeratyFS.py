import debugpy
debugpy.debug_this_thread()

#import concurrent
import gc
import os.path
import time
import copy
import mmap
import hashlib
import traceback
import lzma
import multiprocessing as mp
import math
# from decimal import *
import struct
from hashing import hashed_chunks, hash_data
from concurrent import futures
from stats import Timer, humanbytes, memory
from compression import compressed_pickle, decompress_pickle, decompress_data, compress_data
from BTrees import IOBTree
from collections import OrderedDict, deque
import queue

import sys
import logging
import argparse
import threading
from functools import wraps, lru_cache
from pathlib import Path, PureWindowsPath

from configurations import *

from winfspy import (
    FileSystem,
    BaseFileSystemOperations,
    enable_debug_log,
    FILE_ATTRIBUTE,
    CREATE_FILE_CREATE_OPTIONS,
    NTStatusObjectNameNotFound,
    NTStatusDirectoryNotEmpty,
    NTStatusNotADirectory,
    NTStatusObjectNameCollision,
    NTStatusAccessDenied,
    NTStatusEndOfFile,
    NTStatusMediaWriteProtected,
)
from winfspy.plumbing.win32_filetime import filetime_now
from winfspy.plumbing.security_descriptor import SecurityDescriptor


lock = threading.Lock()


def init_persistance(_fs_meta=None, _keys=None, _datastore=None, _hash=None, _free_blocks=None, _partition_size=None, _GC=None):
    debugpy.debug_this_thread()
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
            f.write(b'\x00\x00\x00\x00\x00')
            f.flush()
            f.close()
            ds = open(chunk_path, "r+b")
            tmp_map = mmap.mmap(ds.fileno(), length=chunk_size, access=mmap.ACCESS_WRITE)
            datastore.append(DataStore(chunk, chunk_size, chunk_path))
            tmp_map.close()
            ds.close()

            free_blocks.append([IOBTree.IOBTree()])

    return datastore, free_blocks, key_index, hash_table, fs_meta


def persist_data(fs_meta=None):
    debugpy.debug_this_thread()
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
    
    
    compressed_pickle(key_index_path, key_index.copy())    
    
    compressed_pickle(hash_table_path, hash_table.copy())
    
    compressed_pickle(free_blocks_path, free_blocks.copy())
    
    compressed_pickle(gc_path, copy.copy(GC))
    
    compressed_pickle(fs_meta_path, fs_meta)

    return True


def get_usage():
    """
    TODO: RECONSIDERAR TROCAR O KEY-INDEX POR UTLIZAÇÃO DE INDICE COM QUANTIDADE NOS BLOCOS
    Return drive virtual (undeduped size) and physical (deduped_size) utilization
    :return: Undeduped Data Un-Compressed, Undeduped Data Compressed, Deduped Data Compressed, Deduped Data Removed, Compression rate
    """
    debugpy.debug_this_thread()
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
    debugpy.debug_this_thread()
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
                hash_table[remove].DELETED = True
                free_blocks[hash_table[remove].chunk].append(hash_table[remove].offset, hash_table[remove])
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
    debugpy.debug_this_thread()
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
    debugpy.debug_this_thread()
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
                            for fb in free_blocks:
                                try:
                                    q.block = fb[0].minKey(len(q.compressed_data)).pop(0)
                                    break
                                except ValueError:
                                    if idx == len(free_blocks) - 1:
                                        print("Partition FULL. No free blocks that can fit the data.")
                                        raise NTStatusAccessDenied
                                    else:
                                        continue
                    written = 0
                    written_hash = ""
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

                        update_index(q.hash, q.chunk, True)

                        registers_processed = registers_processed + 1
                        bytes_processed = bytes_processed + written
                        try:
                            del write_read_cache[q.hash]
                        except KeyError:
                            print('DESGRAÇA')
                        continue
                    else:
                        print(
                            "IOError Writing data to the disk: Chunk:{}, Block:{}, Data Size:{}".format(q.chunk, q.block, len(q.compressed_data)))
                        print(traceback.format_exc())
                        raise IOError

                except Exception:
                    print(traceback.format_exc())

        except IndexError:
            if registers_processed > 0:
                print("DEBUG: Write Queue has been processed. " + str(registers_processed) + " registers")
                print("DEBUG: Processed -> "+humanbytes(bytes_processed)+" in "+humanbytes(bytes_processed/(time.time()-start_time))+" /s")
                persist_data(fs_meta)
            writing = False
            write_buffer_lock = False
            gc.collect()
            break
    write_buffer_lock = False
    # return _queued_writes


def dedup(data):
    debugpy.debug_this_thread()
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
            read_cache[c.hash] = c.data
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
    debugpy.debug_this_thread()
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
            return None


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


def variable_chunks(data):
    return hashed_chunks(data)

""""
FILE SYSTEM OPERATIONS
"""


def operation(fn):
    """Decorator for file system operations.

    Provides both logging and thread-safety
    """
    name = fn.__name__

    @wraps(fn)
    def wrapper(self, *args, **kwargs):
        head = args[0] if args else None
        tail = args[1:] if args else ()
        try:
            with self._thread_lock:
                result = fn(self, *args, **kwargs)
        except Exception as exc:
            logging.info(f" NOK | {name:20} | {head!r:20} | {tail!r:20} | {exc!r}")
            raise
        else:
            logging.info(f" OK! | {name:20} | {head!r:20} | {tail!r:20} | {result!r}")
            return result

    return wrapper


class BaseFileObj:
    @property
    def name(self):
        """File name, without the path"""
        return self.path.name

    @property
    def file_name(self):
        """File name, including the path"""
        return str(self.path)

    def __init__(self, path, attributes, security_descriptor):
        self.path = path
        self.attributes = attributes
        self.security_descriptor = security_descriptor
        now = filetime_now()
        self.creation_time = now
        self.last_access_time = now
        self.last_write_time = now
        self.change_time = now
        self.index_number = 0
        self.file_size = 0
        self.blklst = []  # Blocos no datastore que compoem o arquivo

    def get_file_info(self):
        return {
            "file_attributes": self.attributes,
            "allocation_size": self.allocation_size,
            "file_size": self.file_size,
            "creation_time": self.creation_time,
            "last_access_time": self.last_access_time,
            "last_write_time": self.last_write_time,
            "change_time": self.change_time,
            "index_number": self.index_number,
        }

    def security_descriptor_from_string(self, sec_string):
        self.security_descriptor = SecurityDescriptor.from_string(sec_string)
        return self.security_descriptor

    def __getstate__(self):
        # Copy the object's state from self.__dict__ which contains
        # all our instance attributes. Always use the dict.copy()
        # method to avoid modifying the original state.
        # self.security_descriptor = self.security_descriptor.to_string()
        state = self.__dict__.copy()
        state['security_descriptor'] = self.security_descriptor.to_string()

        # Remove the unpicklable entries.

        return state

    def __setstate__(self, state):
        # Restore instance attributes (i.e., filename and lineno).
        self.__dict__.update(state)
        # Restore the previously opened file's state. To do so, we need to
        # reopen it and read from it until the line count is restored.
        self.security_descriptor = self.security_descriptor_from_string(self.security_descriptor)

    def __repr__(self):
        return f"{type(self).__name__}:{self.file_name}"


class FileObj(BaseFileObj):

    global allocation_unit

    def __init__(self, path, attributes, security_descriptor, allocation_size=0):
        super().__init__(path, attributes, security_descriptor)
        # self.data = bytearray(allocation_size)
        ###
#        self.blklist = []  # Blocos no datastore que compoem o arquivo
        ###
        self.attributes |= FILE_ATTRIBUTE.FILE_ATTRIBUTE_ARCHIVE
        self.allocation_size = allocation_size
        assert not self.attributes & FILE_ATTRIBUTE.FILE_ATTRIBUTE_DIRECTORY

    # @property
    # def allocation_size(self):
    #     s = 0
    #     for b in self.blklst:
    #         s = s + b.size
    #
    #     return s

    def set_allocation_size(self, allocation_size):
        if allocation_size < self.allocation_size:  # TODO: Checar necessidade desta manipulacao
            self.allocation_size = self.file_size+max_blk_size
        elif allocation_size == self.file_size:
            self.allocation_size = self.file_size+max_blk_size
        else:
            self.allocation_size = max(self.allocation_size, self.allocation_size+max_blk_size)
        #     data = self.prepare_file_data()
        #     data = data[:allocation_size]
        #     # self.data = self.data[:allocation_size]
        # if allocation_size > self.allocation_size:
        #     pass
            # self.data += bytearray(allocation_size - self.allocation_size)            
        # assert self.allocation_size == allocation_size
        # self.allocation_size = self.file_size+max_blk_size

        # self.file_size = min(self.file_size, allocation_size)  - Ultimo, voltar esse caso de merda

    def adapt_allocation_size(self, file_size):
        # self.set_allocation_size(file_size)
        # units = (file_size + allocation_unit - 1) // allocation_unit
        # self.set_allocation_size(units * allocation_unit)
        self.set_allocation_size(file_size+max_blk_size)

    def set_file_size(self, file_size):
        if file_size < self.file_size:
            pass
        else:
            self.file_size = file_size
            # zeros = bytearray(self.file_size - file_size)
            # self.data[file_size: self.file_size] = zeros
        if file_size > self.allocation_size:
            self.adapt_allocation_size(file_size)        

    def read(self, offset, length):
        debugpy.debug_this_thread()
        
        if offset >= self.file_size:
            raise NTStatusEndOfFile()
        end_offset = min(self.file_size, offset + length)

        start_blk = 0
        start_diff = 0
        end_blk = 0
        end_diff = 0

        internal_offset = 0
        for blk_number, blk in enumerate(self.blklst):
            internal_offset = internal_offset + blk.size
            if internal_offset < offset:
                continue
            elif internal_offset == offset:
                start_blk = blk_number + 1
                break
            else:
                start_blk = blk_number
                start_diff = internal_offset - offset
                # if start_diff > 32769:
                #     print("tomar no cu")
                break

        internal_offset_e = 0
        for blk_number_e, blk_e in enumerate(self.blklst):
            internal_offset_e = internal_offset_e + blk_e.size
            if internal_offset_e < end_offset:
                continue
            elif internal_offset_e == end_offset:
                end_blk = blk_number_e
                break
            else:
                end_diff = internal_offset_e - end_offset
                end_blk = blk_number_e

                break

        if start_blk == 0 and end_blk == 0:
            return get_file_data(self.blklst, start_blk, end_blk + 1)[offset:end_offset]
        else:
            data = get_file_data(self.blklst, start_blk, end_blk)

            if self.file_size == end_offset:
                data = data[-(end_offset-offset):]
            else:
                if start_diff < 0:
                    print(start_diff)
                if start_blk == 0:
                    data = data[offset:]
                elif offset > 0 and start_diff > 0:
                    data = data[self.blklst[start_blk].size-start_diff:]
                if end_diff > 0 and len(data) > (end_offset-offset):
                    data = data[:-end_diff]

        if data == b'':
            print("Fuck it")

        if len(data) != (end_offset - offset):
            print("data problem")
        # if offset > 546870912:
        #     print(offset)
        return data

    def write(self, buffer, offset, write_to_end_of_file):
        if write_to_end_of_file:
            offset = self.file_size
        end_offset = offset + len(buffer)
        if end_offset > self.file_size:
            self.set_file_size(end_offset)       

        self.blklst += dedup(bytes(buffer))

        self.allocation_size = self.file_size

        return len(buffer)

    # TODO: Re-implementar CONSTRAINED_WRITE
    # def constrained_write(self, buffer, offset):
    #     if offset >= self.file_size:
    #         return 0
    #     end_offset = min(self.file_size, offset + len(buffer))
    #     transferred_length = end_offset - offset
    #     self.data[offset:end_offset] = buffer[:transferred_length]
    #     return transferred_length


class FolderObj(BaseFileObj):
    def __init__(self, path, attributes, security_descriptor):
        super().__init__(path, attributes, security_descriptor)
        self.allocation_size = 0
        assert self.attributes & FILE_ATTRIBUTE.FILE_ATTRIBUTE_DIRECTORY


class OpenedObj:
    def __init__(self, file_obj):
        self.file_obj = file_obj

    def __repr__(self):
        return f"{type(self).__name__}:{self.file_obj.file_name}"


class VeratyFileSystemOperations(BaseFileSystemOperations):
    def __init__(self, volume_label, read_only=False, entries=None):
        super().__init__()
        if len(volume_label) > 31:
            raise ValueError("`volume_label` must be 31 characters long max")

        max_file_nodes = 1024
        max_file_size = partition_size * 1024 * 1024
        # file_nodes = 1

        # "free_size": (max_file_nodes - file_nodes) * max_file_size,

        self._volume_info = {
            "total_size": max_file_nodes * max_file_size,
            "free_size":  (max_file_nodes * max_file_size) - get_usage()[2],
            "volume_label": volume_label,
        }

        self.read_only = read_only
        self._root_path = PureWindowsPath("/")
        self._root_obj = FolderObj(
            self._root_path,
            FILE_ATTRIBUTE.FILE_ATTRIBUTE_DIRECTORY,
            SecurityDescriptor.from_string("O:BAG:BAD:P(A;;FA;;;SY)(A;;FA;;;BA)(A;;FA;;;WD)"),
        )
        if entries is None:
            self._entries = {self._root_path: self._root_obj}
        else:
            self._entries = entries
        self._thread_lock = threading.Lock()

    def get_entries(self):
        return self._entries
    # Debugging helpers

    def _create_directory(self, path):
        path = self._root_path / path
        obj = FolderObj(
            path, FILE_ATTRIBUTE.FILE_ATTRIBUTE_DIRECTORY, self._root_obj.security_descriptor,
        )
        self._entries[path] = obj

    def _import_files(self, file_path):
        file_path = Path(file_path)
        path = self._root_path / file_path.name
        obj = FileObj(
            path, FILE_ATTRIBUTE.FILE_ATTRIBUTE_ARCHIVE, self._root_obj.security_descriptor,
        )
        self._entries[path] = obj
        obj.write(file_path.read_bytes(), 0, False)

    # Winfsp operations

    @operation
    def get_volume_info(self):
        return self._volume_info

    @operation
    def set_volume_label(self, volume_label):
        self._volume_info["volume_label"] = volume_label

    @operation
    def get_security_by_name(self, file_name):
        file_name = PureWindowsPath(file_name)

        # Retrieve file
        try:
            file_obj = self._entries[file_name]
        except KeyError:
            raise NTStatusObjectNameNotFound()

        return (
            file_obj.attributes,
            file_obj.security_descriptor.handle,
            file_obj.security_descriptor.size,
        )

    @operation
    def create(
        self,
        file_name,
        create_options,
        granted_access,
        file_attributes,
        security_descriptor,
        allocation_size,
    ):
        if self.read_only:
            raise NTStatusMediaWriteProtected()

        file_name = PureWindowsPath(file_name)

        # `granted_access` is already handle by winfsp
        # `allocation_size` useless for us

        # Retrieve file
        try:
            parent_file_obj = self._entries[file_name.parent]
            if isinstance(parent_file_obj, FileObj):
                raise NTStatusNotADirectory()
        except KeyError:
            raise NTStatusObjectNameNotFound()

        # File/Folder already exists
        if file_name in self._entries:
            raise NTStatusObjectNameCollision()

        if create_options & CREATE_FILE_CREATE_OPTIONS.FILE_DIRECTORY_FILE:
            file_obj = self._entries[file_name] = FolderObj(
                file_name, file_attributes, security_descriptor
            )
        else:
            file_obj = self._entries[file_name] = FileObj(
                file_name, file_attributes, security_descriptor, allocation_size,
            )

        return OpenedObj(file_obj)

    @operation
    def get_security(self, file_context):
        # print(file_context.file_obj.security_descriptor.to_string())
        return file_context.file_obj.security_descriptor

    @operation
    def set_security(self, file_context, security_information, modification_descriptor):
        if self.read_only:
            raise NTStatusMediaWriteProtected()

        new_descriptor = file_context.file_obj.security_descriptor.evolve(
            security_information, modification_descriptor
        )
        file_context.file_obj.security_descriptor = new_descriptor

    @operation
    def rename(self, file_context, file_name, new_file_name, replace_if_exists):
        if self.read_only:
            raise NTStatusMediaWriteProtected()

        file_name = PureWindowsPath(file_name)
        new_file_name = PureWindowsPath(new_file_name)

        # Retrieve file
        try:
            file_obj = self._entries[file_name]

        except KeyError:
            raise NTStatusObjectNameNotFound()

        if new_file_name in self._entries:
            # Case-sensitive comparison
            if new_file_name.name != self._entries[new_file_name].path.name:
                pass
            elif not replace_if_exists:
                raise NTStatusObjectNameCollision()
            elif not isinstance(file_obj, FileObj):
                raise NTStatusAccessDenied()

        for entry_path in list(self._entries):
            try:
                relative = entry_path.relative_to(file_name)
                new_entry_path = new_file_name / relative
                entry = self._entries.pop(entry_path)
                entry.path = new_entry_path
                self._entries[new_entry_path] = entry
            except ValueError:
                continue

    @operation
    def open(self, file_name, create_options, granted_access):
        file_name = PureWindowsPath(file_name)

        # `granted_access` is already handle by winfsp

        # Retrieve file
        try:
            file_obj = self._entries[file_name]
        except KeyError:
            raise NTStatusObjectNameNotFound()

        return OpenedObj(file_obj)

    @operation
    def close(self, file_context):
        pass

    @operation
    def get_file_info(self, file_context):
        return file_context.file_obj.get_file_info()

    @operation
    def set_basic_info(
        self,
        file_context,
        file_attributes,
        creation_time,
        last_access_time,
        last_write_time,
        change_time,
        file_info,
    ) -> dict:
        if self.read_only:
            raise NTStatusMediaWriteProtected()

        file_obj = file_context.file_obj
        if file_attributes != FILE_ATTRIBUTE.INVALID_FILE_ATTRIBUTES:
            file_obj.attributes = file_attributes
        if creation_time:
            file_obj.creation_time = creation_time
        if last_access_time:
            file_obj.last_access_time = last_access_time
        if last_write_time:
            file_obj.last_write_time = last_write_time
        if change_time:
            file_obj.change_time = change_time

        return file_obj.get_file_info()

    @operation
    def set_file_size(self, file_context, new_size, set_allocation_size):
        if self.read_only:
            raise NTStatusMediaWriteProtected()

        if set_allocation_size:
            file_context.file_obj.set_allocation_size(new_size)
        else:
            file_context.file_obj.set_file_size(new_size)

    @operation
    def can_delete(self, file_context, file_name) -> None:
        file_name = PureWindowsPath(file_name)

        # Retrieve file
        try:
            file_obj = self._entries[file_name]
        except KeyError:
            raise NTStatusObjectNameNotFound

        if isinstance(file_obj, FolderObj):
            for entry in self._entries.keys():
                try:
                    if entry.relative_to(file_name).parts:
                        raise NTStatusDirectoryNotEmpty()
                except ValueError:
                    continue

    @operation
    def read_directory(self, file_context, marker):
        entries = []
        file_obj = file_context.file_obj

        # Not a directory
        if isinstance(file_obj, FileObj):
            raise NTStatusNotADirectory()

        # The "." and ".." should ONLY be included if the queried directory is not root
        if file_obj.path != self._root_path:
            parent_obj = self._entries[file_obj.path.parent]
            entries.append({"file_name": ".", **file_obj.get_file_info()})
            entries.append({"file_name": "..", **parent_obj.get_file_info()})

        # Loop over all entries
        for entry_path, entry_obj in self._entries.items():
            try:
                relative = entry_path.relative_to(file_obj.path)
            # Filter out unrelated entries
            except ValueError:
                continue
            # Filter out ourself or our grandchildren
            if len(relative.parts) != 1:
                continue
            # Add direct chidren to the entry list
            entries.append({"file_name": entry_path.name, **entry_obj.get_file_info()})

        # Sort the entries
        entries = sorted(entries, key=lambda x: x["file_name"])

        # No filtering to apply
        if marker is None:
            return entries

        # Filter out all results before the marker
        for i, entry in enumerate(entries):
            if entry["file_name"] == marker:
                return entries[i + 1 :]

    @operation
    def get_dir_info_by_name(self, file_context, file_name):
        path = file_context.file_obj.path / file_name
        try:
            entry_obj = self._entries[path]
        except KeyError:
            raise NTStatusObjectNameNotFound()

        return {"file_name": file_name, **entry_obj.get_file_info()}

    @operation
    def read(self, file_context, offset, length):
        return file_context.file_obj.read(offset, length)

    @operation
    def write(self, file_context, buffer, offset, write_to_end_of_file, constrained_io):
        if self.read_only:
            raise NTStatusMediaWriteProtected()

        if constrained_io:
            print("write constrained IO")
            return file_context.file_obj.constrained_write(buffer, offset)
        else:
            return file_context.file_obj.write(buffer, offset, write_to_end_of_file)

    @operation
    def cleanup(self, file_context, file_name, flags) -> None:
        debugpy.debug_this_thread()
        
        if self.read_only:
            raise NTStatusMediaWriteProtected()

        # TODO: expose FspCleanupDelete & friends
        FspCleanupDelete = 0x01
        FspCleanupSetAllocationSize = 0x02
        FspCleanupSetArchiveBit = 0x10
        FspCleanupSetLastAccessTime = 0x20
        FspCleanupSetLastWriteTime = 0x40
        FspCleanupSetChangeTime = 0x80
        file_obj = file_context.file_obj

        # Delete
        if flags & FspCleanupDelete:
            # Check for non-empty direcory
            if any(key.parent == file_obj.path for key in self._entries):
                return

            # Delete immediately
            try:
                for b in file_obj.blklst:
                    update_index(b.hash, chunk=None, add=False)
                del self._entries[file_obj.path]
            except KeyError:
                raise NTStatusObjectNameNotFound()

        # Resize
        if flags & FspCleanupSetAllocationSize:
            file_obj.adapt_allocation_size(file_obj.file_size)

        # Set archive bit
        if flags & FspCleanupSetArchiveBit and isinstance(file_obj, FileObj):
            file_obj.attributes |= FILE_ATTRIBUTE.FILE_ATTRIBUTE_ARCHIVE

        # Set last access time
        if flags & FspCleanupSetLastAccessTime:
            file_obj.last_access_time = filetime_now()

        # Set last access time
        if flags & FspCleanupSetLastWriteTime:
            file_obj.last_write_time = filetime_now()

        # Set last access time
        if flags & FspCleanupSetChangeTime:
            file_obj.change_time = filetime_now()

    @operation
    def overwrite(
        self, file_context, file_attributes, replace_file_attributes: bool, allocation_size: int
    ) -> None:
        if self.read_only:
            raise NTStatusMediaWriteProtected()

        file_obj = file_context.file_obj

        # File attributes
        file_attributes |= FILE_ATTRIBUTE.FILE_ATTRIBUTE_ARCHIVE
        if replace_file_attributes:
            file_obj.attributes = file_attributes
        else:
            file_obj.attributes |= file_attributes

        # Allocation size
        file_obj.set_allocation_size(allocation_size)

        # Set times
        now = filetime_now()
        file_obj.last_access_time = now
        file_obj.last_write_time = now
        file_obj.change_time = now

    @operation
    def flush(self, file_context) -> None:
        garbage_collector()
        persist_data(self.get_entries())


def create_memory_file_system(mountpoint, label="memfs", verbose=True, debug=False, testing=False, entries=None):
    if debug:
        enable_debug_log()

    if verbose:
        logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    # The avast workaround is not necessary with drives
    # Also, it is not compatible with winfsp-tests
    mountpoint = Path(mountpoint)
    is_drive = mountpoint.parent == mountpoint
    reject_irp_prior_to_transact0 = not is_drive and not testing

    operations = VeratyFileSystemOperations(label, entries=entries)

    fs = FileSystem(
        str(mountpoint),
        operations,
        sector_size=512,
        sectors_per_allocation_unit=1,
        volume_creation_time=filetime_now(),
        volume_serial_number=0,
        file_info_timeout=1000,
        case_sensitive_search=1,
        case_preserved_names=1,
        unicode_on_disk=1,
        persistent_acls=1,
        post_cleanup_when_modified_only=1,
        um_file_context_is_user_context2=1,
        file_system_name=str(mountpoint),
        prefix="",
        debug=debug,
        reject_irp_prior_to_transact0=reject_irp_prior_to_transact0,
        # security_timeout_valid=1,
        # security_timeout=10000,
    )
    return fs


def main(mountpoint, label, verbose, debug, _meta, _datastore, _size, _hash_table, _free_blocks, _key_index, _allocation_unit, _compression_type, _GC):
    global fs_meta
    global write_buffer
    global write_buffer_lifetime
    global write_buffer_size
    global allocation_unit
    global partition_size
    global lock
    global compressed_file_system
    global free_blocks
    global datastore
    global key_index
    global hash_table
    global write_buffer_lock
    global GC
    global gc_path

    if _compression_type is not None:
        compressed_file_system = _compression_type

    try:
        partition_size = _size
        allocation_unit = _allocation_unit
        datastore, free_blocks, key_index, hash_table, fs_meta = init_persistance(fs_meta, _key_index, _datastore, _hash_table, _free_blocks, _size, _GC)

    except Exception:
        print(traceback.format_exc())
        print("Error initializing persistance. Please check file names and paths provided")
        sys.exit(-1)

    fs = create_memory_file_system(mountpoint, label, verbose, debug, entries=fs_meta)

    try:
        print("Max Partition Size:" + humanbytes(sys.maxsize))
        print("Max File size: " + humanbytes(sys.maxsize//2))
        # '8388608.00 TB'
        print("Starting FS")
        fs.start()
        print("FS started, keep it running forever")

        last_gc = time.time()
        last_commit = time.time()
        persist_data(fs.operations.get_entries())
        while True:
            if time.time() - last_commit > write_buffer_lifetime+5 and not write_buffer_lock:
                write_new_blocks(write_buffer)
                garbage_collector()
                persist_data(fs.operations.get_entries())
                last_commit = time.time()
                time.sleep(5)                

            if time.time() - last_gc > gc_interval:
                print("Used Memory:" + humanbytes(memory()))
                get_usage()                

                # with futures.ThreadPoolExecutor(max_workers=3) as executor:
                #     executor.submit(get_usage, allocation_unit, len(hash_table))
                #     executor.submit(memory)
                #     executor.submit(persist_data, fs.operations.get_entries())


                # persist_data(fs.operations.get_entries())
                # u, d = get_usage()
                # mem = memory()
                # print("-------------------")
                #
                # print("Used Memory:" + humanbytes(mem))
                last_gc = time.time()

                # usage = mp.Process(target=get_usage, args=())
                # usage.start()
                # usage.join()
    except KeyboardInterrupt:
        print("Finalizing File System. Please wait...")
        print("Writing Pending Data...", end='')
        if not write_buffer_lock:
            write_new_blocks(write_buffer)
            garbage_collector()
            persist_data(fs.operations.get_entries())
        else:
            while write_buffer_lock:
                print(".", end='')
                time.sleep(1)
        print('.')
        print('Persisting Metadata....')
        persist_data(fs.operations.get_entries())

    finally:
        print("Stopping FS")
        fs.stop()
        print("FS stopped")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mountpoint")
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("-d", "--debug", action="store_true")
    parser.add_argument("-l", "--label", type=str, default="VeratyFS")
    parser.add_argument("-m", "--meta", type=str, default=None)
    parser.add_argument("-t", "--datastore", type=str, default=None)
    parser.add_argument("-s", "--size", type=int, default=partition_size)  # Size in GB
    parser.add_argument("-k", "--keys", type=str, default=None)
    parser.add_argument("-a", "--hash", type=str, default=None)
    parser.add_argument("-f", "--free_blocks", type=str, default=None)
    parser.add_argument("-u", "--allocation_unit", type=int, default=allocation_unit)  # Allocation Unit in Bytes
    parser.add_argument("-c", "--compression_type", type=str, default=None)
    parser.add_argument("-g", "--garbage_collector", type=str, default=None)
    args = parser.parse_args()
    main(args.mountpoint, args.label, args.verbose, args.debug, args.meta, args.datastore, args.size, args.hash,
         args.free_blocks, args.keys, args.allocation_unit, args.compression_type, args.garbage_collector)
