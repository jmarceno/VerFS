import os.path
import time
import copy
import mmap
import hashlib
import traceback
import lzma
from collections import OrderedDict, deque
from cache import LRU
import multiprocessing as mp
import math
from decimal import *
from stats import Timer, humanbytes, memory
import struct
from compression import compressed_pickle, decompress_pickle, decompress_data, compress_data
"""A memory file system implemented on top of winfspy.

Useful for testing and as a reference.
"""

import sys
import logging
import argparse
import threading
from functools import wraps, lru_cache
from pathlib import Path, PureWindowsPath

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


"""
Bunch of stuff to test the concept. Change this shit later to something useful fast and safe
"""
datastore = None
key_index = {}
hash_table = {}
fs_meta = None
free_blocks = []

under_fetch_limit = 2
over_fetch_limit = 10
cache_size = 20000  # Cache size in entries
read_cache = LRU(maxlen=cache_size)

key_index_path = ""
datastore_path = ""
fs_meta_path = ""
hash_table_path = ""
free_blocks_path = ""

write_buffer = 0
# Buffer de escrita em multiplos da unidade de alocacao
# sendo assim os arquivos serão persistidos a cada X blocos/unidades de alocacao, sendo X o write_buffer_size ou a cada
# Y segundos, sendo Y o write_buffer_lifetime
allocation_unit = 16384
write_buffer_size = 100 * allocation_unit
write_buffer_lifetime = 10
gc_interval = 15  # Intervalo entre o final de uma operação de GC e o inicio de outra
partition_size = 4  # Partion size in GB
compressor_threads = 10
compressed_file_system = None

lock = threading.Lock()

identity_string = b'VeratyFS@v0.0.1@InLineDedup,FixedStoreSize,GC,Compression,FixedBlockSize\n'


def init_persistance(_fs_meta=None, _keys=None, _datastore=None, _hash=None, _free_blocks=None, _partition_size=None):
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
#########################
    if _keys is None:
        key_index_path = "key_index.vfs"
    else:
        key_index_path = _keys

    if os.path.isfile(key_index_path):
        key_index = decompress_pickle(key_index_path)
#########################
    if _hash is None:
        hash_table_path = "hash_table.bin"
    else:
        hash_table_path = _hash

    if os.path.isfile(hash_table_path):
        hash_table = decompress_pickle(hash_table_path)

#########################
    if _free_blocks is None:
        free_blocks_path = "free_blocks.bin"
    else:
        free_blocks_path = _free_blocks

    if os.path.isfile(free_blocks_path):
        free_blocks = decompress_pickle(free_blocks_path)

##########################
    if _fs_meta is None:
        fs_meta_path = "fs.meta"
    else:
        fs_meta_path = _fs_meta

    if os.path.isfile(fs_meta_path):
        fs_meta = decompress_pickle(fs_meta_path)
#########################
    if _datastore is None:
        datastore_path = "datastore.bin"
    else:
        datastore_path = _datastore

    ds_size = partition_size * 1073741824
    if os.path.isfile(datastore_path):
        ds = open(datastore_path, "r+b")
        datastore = mmap.mmap(ds.fileno(), length=ds_size, access=mmap.ACCESS_WRITE)
        #datastore = mmap.mmap(ds.fileno(), length=0, access=mmap.ACCESS_WRITE)
    else:
        f = open(datastore_path, "wb")
        f.write(identity_string)
        f.flush()
        f.close()
        ds = open(datastore_path, "r+b")
        datastore = mmap.mmap(ds.fileno(), length=ds_size, access=mmap.ACCESS_WRITE)
        #datastore = mmap.mmap(ds.fileno(), length=0, access=mmap.ACCESS_WRITE)
        ds.close()

        free_blocks = list(range(0, ds_size, allocation_unit))


def persist_data(fs_meta=None):
    global key_index
    global datastore
    # global fs_meta
    global key_index_path
    global fs_meta_path
    global datastore_path
    global write_buffer
    global write_lock
    global hash_table
    global hash_table_path
    global free_blocks_path
    global lock

    lock.acquire()
    temp_wr_buffer = write_buffer

    keys = key_index.copy()
    compressed_pickle(key_index_path, keys)
    # pickle.dump(keys, open(key_index_path, "wb"))

    h = hash_table.copy()
    compressed_pickle(hash_table_path, h)
    # pickle.dump(h, open(hash_table_path, "wb"))

    f = free_blocks.copy()
    compressed_pickle(free_blocks_path, f)


    #datastore.flush()  # TODO: Mudar para que sejam persistidas apenas as mudanças feitas

    _fs_meta = copy.deepcopy(fs_meta)
    compressed_pickle(fs_meta_path, _fs_meta)
    # pickle.dump(_fs_meta, open(fs_meta_path, "wb"))

    write_buffer = write_buffer - temp_wr_buffer
    lock.release()

    return True


def get_usage():
    """
    Return drive virtual (undeduped size) and physical (deduped_size) utilization
    :return: undeduped_size = Space that should be used if there was no deduplication. deduped_size = physical space being used after the deduplication
    """
    undeduped_size = 0

    tmp = key_index.copy()
    for k in tmp:
        undeduped_size = undeduped_size + (tmp[k]*allocation_unit)

    deduped_size = len(tmp)*allocation_unit

    del tmp

    print("Undeduped Space Used : " + humanbytes(undeduped_size))
    print("Deduped Space Used : " + humanbytes(deduped_size))

    return undeduped_size, deduped_size


def garbage_collector():
    global free_blocks
    global datastore
    global hash_table
    global key_index
    global lock

    to_delete = []
    lock.acquire()

    key_index_copy = key_index.copy()
    for k in key_index_copy:
        if key_index_copy[k] == 0 and k not in free_blocks:
            try:
                free_blocks.append(hash_table[k])
                to_delete.append(k)
                # datastore[k] = None
            except ValueError:
                print("Garbage Collector: Index inconsistance")
                pass

    for i in to_delete:
        del key_index[i]
        del hash_table[i]

    lock.release()


# Checa se a posiçao do datastore já existia no dicionario de indice
# Caso exista, adiciona uma referencia
# Caso não exista, cria nova entrada no dicionário e coloca a quantidade de referncias como 1
# A quantidade de referencias indica quantas vezes aquele bloco esta sendo usado, quanto chegar a zero, ele deve ser
# removido ou sobrescrito
def update_index(idx, add=True):
    """

    :param idx: Hash of the block as of in the hash_table
    :param add: Operation. Should the block usage count go up or down?
    :return:
    """
    global key_index
    global lock
    in_index = False

    lock.acquire()

    if idx in key_index:
        in_index = True

    if add:
        if in_index:
            key_index[idx] = key_index[idx] + 1
        else:
            key_index[idx] = 1
    else:
        try:
            if in_index and key_index[idx] - key_index[idx] <= 0:
                free_blocks.append(hash_table[idx])
                del key_index[idx]
                update_hash_table(_hash=idx, _operation=-1)
            elif in_index:
                key_index[idx] = key_index[idx] - 1
        except Exception:
            print(traceback.format_exc())
            raise IOError
    lock.release()
    return True


def update_hash_table(_hash, _block=None, _operation=1):
    """
    Updates the hash_table in a thread safe way, preventig the dict changes while looping through it

    :param _hash: The hash to be used as the dict key
    :param _block: block number to be used as dict value
    :param _operation: 1: Add , -1:Remove, 0: Update
    """
    global hash_table

    if _operation == 1:
        hash_table[_hash] = _block
    elif _operation == -1:
        del[_hash]
    elif _operation == 0:
        hash_table[_hash] = _block
    else:
        print("Invalid Operation")
        return False

    return True


def hash_data(_data):
    """
    Takes the raw data and calculates it's hash, returning the hexdigest (hex without the leading charecters x0)
    :param _data: data to be hashed
    :return: hexdigest (hex without the leading charecters x0)
    """
    return hashlib.sha3_256(_data).hexdigest()


def write_new_block(_data, _fixed_alloc=True):
    """
    Writes a new block to the datastore
    :param _compression: Compression flag with compression to be used. See "compression.py"
    :param _data: data to be written
    :return: Tuple with the result of the operation and position (block) that the data has been written to
    """
    global datastore

    try:
        if compressed_file_system is not None:
            try:
                _data = compress_data(_data, compressed_file_system)
            except Exception:
                print(traceback.format_exc())
        if _fixed_alloc:
            pos = free_blocks.pop(0)
            datastore.seek(pos)
        else:
            datastore.resize(datastore.size()+len(_data))
            datastore.seek(datastore.size()-len(_data))
            pos = datastore.tell()

        written = datastore.write(_data)

        if len(_data) == written:
            r = True
        else:
            raise IOError
    except ValueError:
        raise IOError
    except TypeError:
        raise NTStatusAccessDenied

    return r, pos


def data_check(_data, _hash):
    """
    Check a chunk of data against its hash
    :param _data: data to be validated
    :param _hash: _hash that the data is expect to conform to
    :return: result of the validation check
    """

    if hash_data(_data) == _hash:
        return True
    else:
        print("Data does no conform to the specified hash")
        return False


def dedup(data, blk_size, check_integrity=False):
    global datastore
    global free_blocks
    global write_lock
    global hash_table
    global lock
    global compressed_file_system

    blk_list = []
    lock.acquire()

    if type(data) == bytearray or type(data) == bytes:
        if type(data) == bytes:
            data = bytearray(data)

        ch = chunks(data, blk_size)
        for c in ch:
            hashed_data = hash_data(c)

            if hashed_data in hash_table:
                blk_list.append(hashed_data)
            else:
                if len(free_blocks) > 0:
                    try:
                        datastore.seek(free_blocks[0])
                        datastore.write(c)
                        pos = free_blocks.pop(0)
                        r = True
                    except ValueError:
                        free_blocks.pop(0)
                        r, pos = write_new_block(c)
                else:
                    r, pos = write_new_block(c)

                if r:
                    blk_list.append(hashed_data)
                    update_hash_table(_hash=hashed_data, _block=pos, _operation=1)
                else:
                    raise IOError

            lock.release()
            update_index(hashed_data)
            lock.acquire()

    else:
        pass
    lock.release()
    return blk_list


def goto_eof():
    global allocation_unit
    global datastore

    for chunk in iter(lambda: datastore.read(allocation_unit), ''):
        if chunk == b'':
            break

    return datastore.tell()


def clip(value, lower, upper):
    return lower if value < lower else upper if value > upper else value


def get_file_data(blklst, start_block=None, end_block=None, check_integrity=False):
    global datastore
    global allocation_unit
    global hash_table
    global compressed_file_system
    global read_cache

    data = bytearray()
    at_start = 0
    for idx, b in enumerate(blklst):
        if idx < start_block:
            at_start = at_start + 1
        elif idx > end_block:
            if at_start > 0:
                return bytearray(at_start*allocation_unit)+data
            else:
                return data
        else:
            cur = hash_table[b]
            if cur+allocation_unit > datastore.size():
                to_read = datastore.size() - cur
                datastore.seek(cur)
                d = datastore.read(to_read-1)
            else:
                datastore.seek(cur)
                d = datastore.read(allocation_unit)

            if d is not None:
                data += d

    if at_start > 0:
        return bytearray(at_start * allocation_unit) + data
    else:
        return data


def seek_in_cache(_blk_hash):
    try:
        return read_cache[_blk_hash]
    except KeyError:
        return None


def single_read(_hash, force=False):
    global datastore
    global allocation_unit
    global hash_table

    cur = hash_table[_hash]
    if cur + allocation_unit > datastore.size():
        to_read = datastore.size() - cur
        datastore.seek(cur)
        d = datastore.read(to_read - 1)
    else:
        datastore.seek(cur)
        d = datastore.read(allocation_unit)

    return d


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

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
        assert not self.attributes & FILE_ATTRIBUTE.FILE_ATTRIBUTE_DIRECTORY


    @property
    def allocation_size(self):
        return len(self.blklst) * allocation_unit
        # return len(get_file_data(self.blklst))
        # return len(self.data)

    def set_allocation_size(self, allocation_size):
        # if allocation_size < self.allocation_size:  # TODO: Checar necessidade desta manipulacao
        #     data = self.prepare_file_data()
        #     data = data[:allocation_size]
        #     # self.data = self.data[:allocation_size]
        # if allocation_size > self.allocation_size:
        #     pass
            # self.data += bytearray(allocation_size - self.allocation_size)
            # self.data += bytearray(allocation_size - self.allocation_size)
        # assert self.allocation_size == allocation_size
        self.file_size = min(self.file_size, allocation_size)

    def adapt_allocation_size(self, file_size):
        units = (file_size + allocation_unit - 1) // allocation_unit
        self.set_allocation_size(units * allocation_unit)

    def set_file_size(self, file_size):
        if file_size < self.file_size:
            pass
            # zeros = bytearray(self.file_size - file_size)
            # self.data[file_size: self.file_size] = zeros
        if file_size > self.allocation_size:
            self.adapt_allocation_size(file_size)
        self.file_size = file_size

    def read(self, offset, length):
        if offset >= self.file_size:
            raise NTStatusEndOfFile()
        end_offset = min(self.file_size, offset + length)

        # START BLOCK --------
        if offset != 0 and offset > allocation_unit:
            start_block = (offset // allocation_unit)  # Bloco inicial.
        else:
            start_block = 0
        # END BLOCK ----------
        if end_offset != self.file_size:
            end_block = (end_offset // allocation_unit)  # Bloco final.
        else:
            end_block = len(self.blklst)

        data = get_file_data(self.blklst, start_block, end_block)

        return data[offset:end_offset]

    def write(self, buffer, offset, write_to_end_of_file):
        if write_to_end_of_file:
            offset = self.file_size
        end_offset = offset + len(buffer)
        if end_offset > self.file_size:
            self.set_file_size(end_offset)

        self.blklst += dedup(bytes(buffer), allocation_unit)
        ###
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
            "free_size":  (max_file_nodes * max_file_size) - get_usage()[0],
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
    def can_delete(self, file_context, file_name: str) -> None:
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
        # file_context.file_obj.prepare_file_data()
        return file_context.file_obj.read(offset, length)

    @operation
    def write(self, file_context, buffer, offset, write_to_end_of_file, constrained_io):
        if self.read_only:
            raise NTStatusMediaWriteProtected()

        if constrained_io:
            return file_context.file_obj.constrained_write(buffer, offset)
        else:
            return file_context.file_obj.write(buffer, offset, write_to_end_of_file)

    @operation
    def cleanup(self, file_context, file_name, flags) -> None:
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
                    update_index(b, False)
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


def main(mountpoint, label, verbose, debug, _meta, _datastore, _size, _hash_table, _free_blocks, _key_index, _allocation_unit, _compression_type):
    global fs_meta
    global write_lock
    global write_buffer
    global write_buffer_lifetime
    global write_buffer_size
    global allocation_unit
    global partition_size
    global lock
    global compressed_file_system

    if _compression_type is not None:
        compressed_file_system = _compression_type

    try:
        partition_size = _size
        allocation_unit = _allocation_unit
        init_persistance(fs_meta, _key_index, _datastore, _hash_table, _free_blocks, _size)
    except Exception:
        print("Error initializing persistance. Please check file names and paths provided")

    fs = create_memory_file_system(mountpoint, label, verbose, debug, entries=fs_meta)

    try:
        print("Starting FS")
        fs.start()
        print("FS started, keep it running forever")

        last_write = time.time()
        last_gc = time.time()
        persist_data(fs.operations.get_entries())
        while True:
            time.sleep(5)
            if time.time() - last_gc > gc_interval and not lock.locked():
                # garbage_collector()
                persist_data(fs.operations.get_entries())
                u, d = get_usage()
                mem = memory()
                print("-------------------")
                #print("Undeduped Space Used : " + humanbytes(u))
                #print("Deduped Space Used : " + humanbytes(d))
                print("Used Memory:" + humanbytes(mem))
                last_gc = time.time()

                # usage = mp.Process(target=get_usage, args=())
                # usage.start()
                # usage.join()

    finally:
        print("Stopping FS")
        fs.stop()
        print("FS stopped")


if __name__ == "__main__":
    mp.set_start_method('spawn')
    parser = argparse.ArgumentParser()
    parser.add_argument("mountpoint")
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("-d", "--debug", action="store_true")
    parser.add_argument("-l", "--label", type=str, default="VeratyFS")
    parser.add_argument("-m", "--meta", type=str, default=None)
    parser.add_argument("-t", "--datastore", type=str, default=None)
    parser.add_argument("-s", "--size", type=int, default=4)  # Size in GB
    parser.add_argument("-k", "--keys", type=str, default=None)
    parser.add_argument("-a", "--hash", type=str, default=None)
    parser.add_argument("-f", "--free_blocks", type=str, default=None)
    parser.add_argument("-u", "--allocation_unit", type=int, default=4096)  # Allocation Unit in Bytes
    parser.add_argument("-c", "--compression_type", type=str, default=None)
    args = parser.parse_args()
    main(args.mountpoint, args.label, args.verbose, args.debug, args.meta, args.datastore, args.size, args.hash,
         args.free_blocks, args.keys, args.allocation_unit, args.compression_type)
