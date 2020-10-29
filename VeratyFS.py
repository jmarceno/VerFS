#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
VeratyFS File System
- Version 0.0.5
- Variable Block Size
- Inline Dedup at Block Level
- Transparent InLine Compression - Fast compression algorithim which only compress data above a certain threshold
- 
- Planned:
- Software RAID
- Cloud Sync and Mount
- Posix Compliant
'''
# import tracemalloc

# tracemalloc.start()

import os
import sys
import multiprocessing as mp
import queue
import random
# If we are running from the pyfuse3 source directory, try
# to load the module from there first.
basedir = os.path.abspath(os.path.join(os.path.dirname(sys.argv[0]), '..'))
if (os.path.exists(os.path.join(basedir, 'setup.py')) and
    os.path.exists(os.path.join(basedir, 'src', 'pyfuse3.pyx'))):
    sys.path.insert(0, os.path.join(basedir, 'src'))

import time
import copy
import mmap
import hashlib
import lzma
import math
import struct
from hashing import hashed_chunks, hash_data
from stats import Timer, humanbytes, memory
from compression import compressed_pickle, decompress_pickle, decompress_data, compress_data
from BTrees import IOBTree
from collections import OrderedDict, deque
from functools import lru_cache
import pyfuse3
import errno
import stat
from time import time
import logging
from collections import defaultdict
from pyfuse3 import FUSEError
from argparse import ArgumentParser
import trio
import traceback
from psutil import virtual_memory
import gc

from datastructures import *
from configurations import *
from persistence import init_persistance, persist_data
from stats import unix_memory, resident, stacksize
import mq_client

# datastore, free_blocks, key_index, hash_table, fs_meta, GC = init_persistance()

write_buffer_lock = False
LOCKED_WRITE = False


# try:
#     if fs_meta[0] is not None:
#         inodes = fs_meta[0]
#         contents = fs_meta[1]
#     else:
#         inodes = inodes
#         contents = contents
# except:
#         inodes = inodes
#         contents = contents

try:
    import faulthandler
except ImportError:
    pass
else:
    faulthandler.enable()

log = logging.getLogger()

class Operations(pyfuse3.Operations):
    '''An example filesystem that stores all data in memory
    TODO: REVIEW THIS PROBLEMS
    This is a very simple implementation with terrible performance.
    Don't try to store significant amounts of data. Also, there are
    some other flaws that have not been fixed to keep the code easier
    to understand:

    * atime, mtime and ctime are not updated
    * generation numbers are not supported
    * lookup counts are not maintained
    '''    

    global fs_meta
    
    enable_writeback_cache = False
    
    def __init__(self, stat_msg_queue):
        super(Operations, self).__init__()
        
        self.inode_open_count = defaultdict(int)
        self.stat_msg_queue = stat_msg_queue
                
        try:
            if fs_meta is not None and fs_meta[0] is None or fs_meta[1] is None:
                self.inodes = inodes
                self.contents = contents
                self.init_file_system()
            else:
                self.inodes = fs_meta[0]
                self.contents = fs_meta[1]
        except TypeError:
            self.inodes = inodes
            self.contents = contents
            self.init_file_system()           


    def init_file_system(self):
        '''Initialize file system '''

        now_ns = int(time.time_ns())
        new_file = File_Inode(pyfuse3.ROOT_INODE)        
        new_file.mode = stat.S_IFDIR | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR | stat.S_IRGRP | stat.S_IXGRP | stat.S_IROTH | stat.S_IXOTH
        new_file.uid = os.getuid()
        new_file.gid = os.getgid()
        new_file.mtime_ns = now_ns
        new_file.atime_ns = now_ns
        new_file.ctime_ns = now_ns
        self.inodes[pyfuse3.ROOT_INODE] = new_file

        root_dir = Directory_Inode(pyfuse3.ROOT_INODE)
        root_dir.name = b'..'
        root_dir.parent_inode = pyfuse3.ROOT_INODE
        root_dir.inode = pyfuse3.ROOT_INODE
        self.contents[pyfuse3.ROOT_INODE] = root_dir


    async def lookup(self, inode_p, name, ctx=None):
        inode = None
        if name == '.':
            inode = inode_p
        elif name == '..':
            inode = self.contents[inode_p]
        elif str(b'.Trash') in str(name):
            raise(pyfuse3.FUSEError(errno.ENOENT))
        else:
            try:                
                for i in self.contents:
                    if self.contents[i].name == name and self.contents[i].parent_inode ==  inode_p:
                        inode = self.contents[i].inode
                        break                        
            except TypeError:                
                raise(pyfuse3.FUSEError(errno.ENOENT))
            except AttributeError:
                raise(pyfuse3.FUSEError(errno.ENOENT))
            except KeyError:
                raise (pyfuse3.FUSEError(errno.ENOENT))
            
            if inode is None:
                raise(pyfuse3.FUSEError(errno.ENOENT))

        return await self.getattr(inode, ctx)

    async def getattr(self, inode, ctx=None):        

        try:
            f = self.inodes[inode]

            entry = pyfuse3.EntryAttributes()
            entry.st_ino = inode
            entry.generation = 0
            entry.entry_timeout = 300
            entry.attr_timeout = 300
            entry.st_mode = f.mode

            entry.st_nlink = 1 #await self.count_entries(inode) # #TODO O sistema vai suportar HARD-LINKS? Caso negativo, apenas retornar 2, funciona

            entry.st_uid = f.uid
            entry.st_gid = f.gid
            entry.st_rdev = f.rdev
            entry.st_size = f.size

            entry.st_blksize = allocation_unit
            entry.st_blocks = 1
            entry.st_atime_ns = f.atime_ns
            entry.st_mtime_ns = f.mtime_ns
            entry.st_ctime_ns = f.ctime_ns

            del f
        except KeyError:
            raise (pyfuse3.FUSEError(errno.ENOENT))

        return entry


    async def readlink(self, inode, ctx):
        return self.inodes[inode].target        

    async def opendir(self, inode, ctx):
        return inode

    async def readdir(self, inode, off, token):
        dir_entries = []
        [dir_entries.append(self.contents[x]) for y, x in enumerate(self.contents,off) if self.contents[x].parent_inode == inode]
        # for d in self.contents:
        #     if self.contents[d].parent_inode == inode:
        #         dir_entries.append(self.contents[d])

        try:
            pyfuse3.readdir_reply(token, dir_entries[off].name, await self.getattr(dir_entries[off].inode), off+1)
        except IndexError:
            return False

    async def unlink(self, inode_p, name,ctx):
        entry = await self.lookup(inode_p, name)

        if stat.S_ISDIR(entry.st_mode):
            raise pyfuse3.FUSEError(errno.EISDIR)

        self._remove(inode_p, name, entry)

    async def rmdir(self, inode_p, name, ctx):
        entry = await self.lookup(inode_p, name)

        if not stat.S_ISDIR(entry.st_mode):
            raise pyfuse3.FUSEError(errno.ENOTDIR)

        self._remove(inode_p, name, entry)

    def _remove(self, inode_p, name, entry):
        if self.contents[inode_p].inode != inode_p and self.contents[inode_p].parent_inode == inode_p and self.contents[inode_p].name != name:
            raise pyfuse3.FUSEError(errno.ENOTEMPTY)
        
        for e in list(self.contents.keys()): #TODO: LENTO MUDAR
            if self.contents[e].name == name and self.contents[e].parent_inode == inode_p:
                self.contents.pop(e)
                break

        if entry.st_nlink == 1 and entry.st_ino not in self.inode_open_count:
            try:
                removed = self.inodes.pop(entry.st_ino)
                for b in removed.data:
                    update_index(b.hash, chunk=None, add=False)
            except KeyError:
                print(traceback.format_exc())
                print("Key already removed")

    async def symlink(self, inode_p, name, target, ctx):
        mode = (stat.S_IFLNK | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR |
                stat.S_IRGRP | stat.S_IWGRP | stat.S_IXGRP |
                stat.S_IROTH | stat.S_IWOTH | stat.S_IXOTH)
        return await self._create(inode_p, name, mode, ctx, target=target)

    async def rename(self, inode_p_old, name_old, inode_p_new, name_new,
                     flags, ctx):
        if flags != 0:
            raise FUSEError(errno.EINVAL)

        entry_old = await self.lookup(inode_p_old, name_old)

        try:
            entry_new = await self.lookup(inode_p_new, name_new)
        except pyfuse3.FUSEError as exc:
            if exc.errno != errno.ENOENT:
                raise
            target_exists = False
        else:
            target_exists = True

        if target_exists:
            self._replace(inode_p_old, name_old, inode_p_new, name_new,
                          entry_old, entry_new)
        else:
            old = self.contents[inode_p_old]
            old.name = name_new
            old.parent_inode = inode_p_new
            self.contents[inode_p_old] = old
            # self.inodes[inode_p_old] = old


    def _replace(self, inode_p_old, name_old, inode_p_new, name_new,
                 entry_old, entry_new):

        for c in self.contents:            
            if self.contents[c].inode != inode_p_old and self.contents[c].parent_inode == inode_p_old and self.contents[c].name != name_old and str('.trashinfo.') not in str(name_new) and str('.trashinfo.') not in str(name_old):
                raise pyfuse3.FUSEError(errno.ENOTEMPTY)
        
        old = self.contents.pop(inode_p_old)
        new_d = Directory_Inode(self.contents)
        new_d.name = old.name
        new_d.inode = entry_old.st_ino
        new_d.parent_inode = old.parent_inode
        self.contents[new_d.inode] = new_d

        # if entry_new.st_nlink == 1 and entry_new.st_ino not in self.inode_open_count:
        #     for i in list(self.inodes.keys()):
        #         if self.inodes.get(i).id == entry_new.st_ino:
        #             removed = self.inodes.pop(i)
        #             for b in removed.data:
        #                 update_index(b.hash, chunk=None, add=False)


    async def link(self, inode, new_inode_p, new_name, ctx):
        entry_p = await self.getattr(new_inode_p)
        if entry_p.st_nlink == 0:
            log.warning('Attempted to create entry '+ str(new_name) + 'with unlinked parent '+ str(new_inode_p))
            raise FUSEError(errno.EINVAL)

        d = Directory_Inode(len(self.contents))
        d.name = new_name
        d.inode = inode
        d.parent_inode = new_inode_p
        self.contents[inode] = d

        return await self.getattr(inode)

    async def setattr(self, inode, attr, fields, fh, ctx):

        old_inode = self.inodes[inode]

        if fields.update_size:
            size = 0
            f = self.inodes[inode]
            for d in f.data:
                size = size + d.size
            if size > 0:
                old_inode.size = size
            
        if fields.update_mode:
            old_inode.mode = attr.st_mode          

        if fields.update_uid:
            old_inode.uid = attr.st_uid
            
        if fields.update_gid:
            old_inode.gid = attr.st_gid   
        
        if fields.update_atime:
            old_inode.atime_ns = attr.st_atime_ns            

        if fields.update_mtime:
            old_inode.mtime_ns = attr.st_mtime_ns            

        if fields.update_ctime:
            old_inode.ctime_ns = attr.st_ctime_ns
            
        else:
            old_inode.ctime_ns = time.time_ns()
            
        self.inodes[inode] = old_inode

        return await self.getattr(inode)

    async def mknod(self, inode_p, name, mode, rdev, ctx):
        return await self._create(inode_p, name, mode, ctx, rdev=rdev)

    async def mkdir(self, inode_p, name, mode, ctx):
        return await self._create(inode_p, name, mode, ctx)

    async def statfs(self, ctx):
        stat_ = pyfuse3.StatvfsData()

        stat_.f_bsize = allocation_unit
        stat_.f_frsize = allocation_unit

        size = 0
        for k in list(self.inodes.keys()):
            size = size + self.inodes[k].size
        stat_.f_blocks = max(0, (partition_size_gb // (stat_.f_frsize//dummy_mult)))
        # stat_.f_blocks = size // stat_.f_frsize
        stat_.f_bfree = max(0, (partition_size_gb - get_usage(self.stat_msg_queue)[2]) // allocation_unit )#stat_.f_blocks - (size // allocation_unit)
        # stat_.f_bfree = max(size // stat_.f_frsize, 1024) #TODO: WHAT IS THIS SHIT?
        stat_.f_bavail = stat_.f_bfree

        fs_inodes = len(self.inodes)
        stat_.f_files = fs_inodes
        stat_.f_ffree = stat_.f_bfree
        stat_.f_favail = stat_.f_ffree

        return stat_

    async def open(self, inode, flags, ctx):
        # Yeah, unused arguments
        #pylint: disable=W0613
        self.inode_open_count[inode] += 1

        # Use inodes as a file handles
        return pyfuse3.FileInfo(fh=inode, direct_io=True, keep_cache=False)

    async def access(self, inode, mode, ctx):
        # Yeah, could be a function and has unused arguments
        #pylint: disable=R0201,W0613
        return True

    async def create(self, inode_parent, name, mode, flags, ctx):
        #pylint: disable=W0612
        entry = await self._create(inode_parent, name, mode, ctx)
        self.inode_open_count[entry.st_ino] += 1
        return (pyfuse3.FileInfo(fh=entry.st_ino, direct_io=True, keep_cache=False), entry)

    async def _create(self, inode_p, name, mode, ctx, rdev=0, target=None):
        if (await self.getattr(inode_p)).st_nlink == 0:
            log.warning('Attempted to create entry '+ str(name) + 'with unlinked parent '+ str(inode_p))
            raise FUSEError(errno.EINVAL)

        inode = len(self.inodes)+1
        now_ns = time.time_ns()        
        new_file = File_Inode(inode)
        new_file.mode = mode
        new_file.uid = ctx.uid
        new_file.gid = ctx.gid
        new_file.mtime_ns = now_ns
        new_file.atime_ns = now_ns
        new_file.ctime_ns = now_ns
        new_file.target = target
        new_file.rdev = rdev
        self.inodes[new_file.id] = new_file
        
        d = Directory_Inode(inode)
        d.name = name
        d.parent_inode = inode_p

        self.contents[inode] = d
        
        return await self.getattr(inode)

    
    async def read(self, fh, offset, length): 
        start = time.time()       
        # f = None

        # f = [x for x in self.inodes.values()  if x.id == fh]

        # for i in list(self.inodes.keys()):
        #     if self.inodes[i].id == fh:
        #         f = self.inodes[i]
        #         break

        if len(self.inodes[fh].data) == 0:
            data = b''
        
        else:        
            # if offset >= self.inodes[fh].size:
            #     print("End of File")
            #     raise IOError
            end_offset = min(self.inodes[fh].size, offset + length)

            start_blk = 0
            start_diff = 0
            end_blk = 0
            end_diff = 0

            internal_offset = 0
            for blk_number, blk in enumerate(self.inodes[fh].data):
                internal_offset = internal_offset + blk.size
                if internal_offset < offset:
                    continue
                elif internal_offset == offset:
                    start_blk = blk_number + 1
                    break
                else:
                    start_blk = blk_number
                    start_diff = internal_offset - offset                    
                    break

            internal_offset_e = 0
            for blk_number_e, blk_e in enumerate(self.inodes[fh].data):
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
                data = get_file_data(self.stat_msg_queue, self.inodes[fh].data, start_blk, end_blk + 1)[offset:end_offset]
                print("Time taken -> " + str(time.time()-start) + " Data Length:" + str(len(data)))                       
                return data
            else:
                data = get_file_data(self.stat_msg_queue, self.inodes[fh].data, start_blk, end_blk)

                if self.inodes[fh].size == end_offset:
                    data = data[-(end_offset-offset):]
                else:
                    if start_diff < 0:
                        print(start_diff)
                    if start_blk == 0:
                        data = data[offset:]
                    elif offset > 0 and start_diff > 0:
                        data = data[self.inodes[fh].data[start_blk].size-start_diff:]
                    if end_diff > 0 and len(data) > (end_offset-offset):
                        data = data[:-end_diff]

            # if data == b'':
            #     print("Returning Empty data @ [async def read]. Is this intended?")

            if len(data) != (end_offset - offset):
                print("Returning wrong length data @ [async def read]. Is this intended?")
                return data

        if data is None:
            data = b''

        print("Time taken -> " + str(time.time()-start) + " Data Length:" + str(len(data)))

        return data
    
    async def write(self, fh, offset, buf):        
        buf = memoryview(buf)
        f = None
        for i in list(self.inodes.keys()):
            if self.inodes.get(i).id == fh:
                f = self.inodes[i]
                break

        end_offset = offset + len(buf)
        if end_offset > f.size:
            f.size = end_offset #TODO: SETAR TAMANHO DO ARQUIVO DE FORMA CORRETA

        f.data += dedup(buf, self.stat_msg_queue)
        
        self.inodes[fh].data = f.data
        self.inodes[fh].size = f.size
        # self.inodes.update({fh, f})       
        
        inodes = self.inodes    # Coloca os dados do FS de volta na memória compartilhada para pode ser acessada e persistida
        contents = self.contents    #TODO: TROCAR A FORMA DE USO PARA NAO PRECISAR MANTER 2 VARIAVEIS NA MEMORIA
        
        # snapshot = tracemalloc.take_snapshot()
        # top_stats = snapshot.statistics('lineno', cumulative=True)
        # os.system('clear')
        # print("[ Top 10 ]")
        # for stat in top_stats[:20]:
        #     print(stat)

        return len(buf)

    async def release(self, fh):
        self.inode_open_count[fh] -= 1

        if self.inode_open_count[fh] == 0:
            del self.inode_open_count[fh]
            if (await self.getattr(fh)).st_nlink == 0:
                self.inodes.pop(fh)


'''

VERATYFS OPERATIONS AND NON OS DEPENDENT CODE
DEDUP, COMPRESSION, WRITE, DELETE, INDEXES MAINTAINANCE

'''

def get_small_blocks_real_size(start_path = '.'):
    total_size = 0
    for dirpath, dirnames, filenames in os.walk(start_path):
        for f in filenames:
            fp = os.path.join(dirpath, f)
            # skip if it is symbolic link
            if not os.path.islink(fp):
                total_size += os.path.getsize(fp)

    return total_size

def get_usage(stat_msg_queue):
    """
    TODO: RECONSIDERAR TROCAR O KEY-INDEX POR UTLIZAÇÃO DE INDICE COM QUANTIDADE NOS BLOCOS
    Return drive virtual (undeduped size) and physical (deduped_size) utilization
    :return: Undeduped Data Un-Compressed, Undeduped Data Compressed, Deduped Data Compressed, Deduped Data Removed, Compression rate
    """    
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
                undeduped_compressed = undeduped_compressed + (max(0, key_index_copy[k]) * hash_table[k].size)
                deduped_compressed = deduped_compressed + hash_table[k].size
        except KeyError:
            continue
    del key_index_copy
    
    fragmenation_size = fragmentation['free_size']
    fragmenation_quant = fragmentation['free_count']
   
    if undeduped_compressed != 0 and undeduped_uncompressed != 0:
        compression_rate = undeduped_compressed/undeduped_uncompressed
    
    
    stat_msg_queue.put({'STAT:TOTAL_SIZE':partition_size_gb})
    stat_msg_queue.put({'STAT:Undeduped (No Compression) Space Used':undeduped_uncompressed})        
    stat_msg_queue.put({'STAT:Free Space': (max(0,(partition_size_gb - (deduped_compressed))))})
    stat_msg_queue.put({'STAT:Undeduped (Compression) Space Used':undeduped_compressed})
    stat_msg_queue.put({'STAT:Deduped (Compression) Space Used':deduped_compressed})
    stat_msg_queue.put({'STAT:Compression Rate':1 - compression_rate})
    stat_msg_queue.put({'STAT:Total Savings':(undeduped_uncompressed - undeduped_compressed)+(undeduped_uncompressed -deduped_compressed)})
    stat_msg_queue.put({'STAT:Fragmentation':max(0, fragmentation['free_size'])})
    stat_msg_queue.put({'STAT:Fragmented Blocks':fragmentation['free_count']})

    gc.collect()

    return undeduped_uncompressed, undeduped_compressed, deduped_compressed, (undeduped_compressed-deduped_compressed), compression_rate


def garbage_collector(stat_msg_queue):    
    global free_blocks
    global hash_table
    global key_index
    global fragmentation
    
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
                if hash_table[remove].size <= small_block_limit:
                    r = delete_small_block(remove, stat_msg_queue)
                    if not r:
                        print("DEBUG: Block could not be deleted. File "+ str(remove) +" is now orphan. Please manually delete.")                
                else:
                    try:
                        free_blocks[hash_table[remove].chunk].get(hash_table[remove].size).append(hash_table[remove].offset)                    
                    except AttributeError:
                        free_blocks[hash_table[remove].chunk].insert(hash_table[remove].size, [hash_table[remove].offset])
                    finally:
                        fragmentation['free_size'] = fragmentation['free_size'] + hash_table[remove].size
                        fragmentation['free_count'] = fragmentation['free_count'] + 1            
            else:
                if random.randrange(1,q_random) == 1:
                    stat_msg_queue.put({'INFO:GCProgress' : str((len(GC.add_uses) + len(GC.remove_uses)))})

    except IndexError:
        pass


# Checa se a posiçao do datastore já existia no dicionario de indice
# Caso exista, adiciona uma referencia
# Caso não exista, cria nova entrada no dicionário e coloca a quantidade de referncias como 1
# A quantidade de referencias indica quantas vezes aquele bloco esta sendo usado, quanto chegar a zero, ele deve ser
# removido ou sobrescrito

def update_index(idx, chunk=None, add=True, stat_msg_queue=None):
    """

    :param chunk:
    :param idx: Hash of the block as of in the hash_table
    :param add: Operation. Should the block usage count go up or down?
    :return:
    """    
    global key_index
    global free_blocks

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
                if free_blocks[hash_table[idx].chunk].get(hash_table[idx].size) is not None:
                    free_blocks[hash_table[idx].chunk].get(hash_table[idx].size).append(hash_table[idx].offset)                    
                else:
                    free_blocks[hash_table[idx].chunk].insert(hash_table[idx].size, [hash_table[idx].offset])                    
                    
                GC.remove_uses.append(idx)
                hash_table[idx].DELETED = True
                hash_table[idx].DELETION_TIME = time.time()
                try:
                    del key_index[idx]
                    del read_cache[idx]
                except KeyError:
                    pass                
            elif in_index:
                key_index[idx] = key_index[idx] - 1
                hash_table[idx].uses = hash_table[idx].uses - -1
        except Exception:
            print(traceback.format_exc())
            raise IOError

    return True


def write_new_blocks(_queued_writes, wq, resq, swq, stat_msg_queue):
    """
    Writes a series of blocks that where quede

    :param _queued_writes: data to be written
    :return: Tuple with the result of the operation and position (block) that the data has been written to
    """    
    global chunk_size
    global datastore
    global free_blocks
    global write_buffer_lock
    global hash_table    
    
    write_buffer_lock = True
    registers_processed = 0
    bytes_processed = 0
    writing = True    
    start_time = time.time()

    
    while writing:
        try:            
            q = write_buffer.popleft()
            if not q.result and q.hash not in hash_table:
                try:
                    if len(q.compressed_data) <= small_block_limit:
                        if q.compressed:                            
                            swq.put((q.hash, q.compressed_data))                                                        
                            q.chunk = -1                            
                        else:                            
                            swq.put((q.hash, q.data))                            
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
                                            fragmentation['free_size'] = fragmentation['free_size'] - s
                                            fragmentation['free_count'] = fragmentation['free_count'] - 1                                   
                                        break
                                    except ValueError:
                                        if ds == len(free_blocks) - 1:
                                            print("Partition FULL. No free blocks that can fit the data.")
                                            raise IOError
                                        else:
                                            continue                    
                        try:
                            wq.put_nowait((q, datastore))
                        except:
                            print('Error sending data to the flusing queue')
                            print(traceback.format_exc())
                       
                    q.result = True                    
                    hash_table[q.hash] = Block()
                    hash_table[q.hash].hash = q.hash
                    hash_table[q.hash].chunk = q.chunk
                    hash_table[q.hash].offset = q.block
                    hash_table[q.hash].size = len(q.compressed_data)
                    hash_table[q.hash].deflated_size = len(q.data)
                    hash_table[q.hash].compressed = q.compressed
                    update_index(q.hash, q.chunk, True, stat_msg_queue)

                    if len(q.compressed_data) <= small_block_limit:
                        small_block_read_cache[q.hash] = q.data

                    try:
                        del write_read_cache[q.hash]
                    except KeyError:
                        continue

                    registers_processed = registers_processed + 1
                    if q.compressed:
                        bytes_processed = bytes_processed + len(q.compressed_data)
                    else:
                        bytes_processed = bytes_processed + len(q.data)                    
                    continue                    

                except Exception:
                    print(traceback.format_exc()) 
            else:                
                registers_processed = registers_processed + 1                
                if q.compressed:
                    bytes_processed = bytes_processed + len(q.compressed_data)
                else:
                    bytes_processed = bytes_processed + len(q.data)                
            del q

        except IndexError:
            writing = False
            write_buffer_lock = False
    
    write_buffer_lock = False
    
    if registers_processed > 0:
        pass
        # else:
        #     if random.randrange(1,q_random) == 1:
        # stat_msg_queue.put({'INFO:WriteSpeed' : bytes_processed/(time.time()-start_time)})
            # print(humanbytes(bytes_processed/(time.time()-start_time))+" /s")
        # pass
        # print("DEBUG: Write Queue has been processed. " + str(registers_processed) + " registers")
        # print("DEBUG: Processed -> "+humanbytes(bytes_processed)+" in "+humanbytes(bytes_processed/(time.time()-start_time))+" /s")    
    


def dedup(data, stat_msg_queue):    
    global datastore
    global free_blocks    
    global hash_table    
    global read_cache
    global write_buffer
    global write_read_cache

    blk_list = []
    queued_writes = []

    start_time = time.time()
    bytes_processed = 0

    if type(data) == bytearray or type(data) == bytes or type(data) == memoryview:
        if type(data) != memoryview:
            data = bytearray(data)
        
        if len(data) <= min_blk_size:
            _hashed_data = hash_data(data)
            blk_list.append(0)
            if _hashed_data in hash_table:
                blk_list[len(blk_list)-1] = FileBlock(_hashed_data, len(data))
                update_index(_hashed_data)
            else:                
                q = QueuedWrite(len(blk_list)-1, _hashed_data, bytearray(data))
                blk_list[q.idx] = FileBlock(_hashed_data, len(data))
                write_read_cache[_hashed_data] = data
                write_buffer.append(q)
                bytes_processed = bytes_processed + len(data)
        else:
            ch = variable_chunks(data)
            for c in ch:
                blk_list.append(0)

                if c.hash in hash_table:
                    blk_list[len(blk_list)-1] = FileBlock(c.hash, len(c.data))
                    update_index(c.hash)
                else:                
                    q = QueuedWrite(len(blk_list)-1, c.hash, bytearray(c.data))
                    blk_list[q.idx] = FileBlock(c.hash, len(c.data))
                    write_read_cache[c.hash] = c.data
                    write_buffer.append(q)

                bytes_processed = bytes_processed + len(c.data)

    else:
        print("Value Error when preparing writes")
        print(traceback.format_exc())
        raise ValueError
    
    if random.randrange(0,10) == 1:
        stat_msg_queue.put({'INFO:DedupSpeed' : bytes_processed/ (time.time()- start_time)})
    
    return blk_list


def get_file_data(stat_msg_queue, blklst, start_block=None, end_block=None, offset=0, end_offset=0):    
    global datastore
    global allocation_unit
    global hash_table
    global read_cache

    data = bytearray()

    cached = False    
    r = bytearray()
    
    start_time = time.time()
    bytes_processed = 0

    for idx, b in enumerate(blklst):
        if idx < start_block:
            continue            
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
        
        if d is None and hash_table[b.hash].chunk == -1:
            try:
                d = read_small_block(b.hash, stat_msg_queue)
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
                time.sleep(0.001)
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
                    # mm.madvise(mmap.MADV_DONTNEED)
                    mm.close()
                    # del mm
                                    
                d = r[:read_size]
                r = r[read_size:]
                
                if hash_table[b.hash].compressed:
                        decompresed_data = decompress_data(d)   # check if the data has been compressed or not. If it was, decompress it, otherwise return data as read
                else:
                    decompresed_data = d

                    read_cache[_hash] = decompresed_data
                
                d = decompresed_data

            else:
                print("IOError when trying to read physical media")
                print(traceback.format_exc())
                raise IOError

        if b.hash != hash_data(d):
            print('Hash of the data at [def get_file_data], from requested location, does not seem to match the requested hash')
        if d is not None:                
            data += d
            bytes_processed = bytes_processed + len(d)
            
    stat_msg_queue.put({'INFO:ReadSpeed' : str(bytes_processed/ (time.time()- start_time))})
    
    try:        
        del cached
        del r    
        del start_time
        del bytes_processed
        del d
        del idx
        del b
    except:
        pass

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

'''

CODE INITIALIZATION AND RUN

'''

def init_logging(debug=False):
    formatter = logging.Formatter('%(asctime)s.%(msecs)03d %(threadName)s: '
                                  '[%(name)s] %(message)s', datefmt="%Y-%m-%d %H:%M:%S")
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    if debug:
        handler.setLevel(logging.DEBUG)
        root_logger.setLevel(logging.DEBUG)
    else:
        handler.setLevel(logging.INFO)
        root_logger.setLevel(logging.INFO)
    root_logger.addHandler(handler)

def parse_args():
    '''Parse command line'''

    parser = ArgumentParser()

    parser.add_argument('mountpoint', type=str,
                        help='Where to mount the file system')
    parser.add_argument('--debug', action='store_true', default=False,
                        help='Enable debugging output')
    parser.add_argument('--debug-fuse', action='store_true', default=False,
                        help='Enable FUSE debugging output')

    return parser.parse_args()


async def parent(stat_msg_queue):    

    print("Starting main process coodenator...")
    async with trio.open_nursery() as nursery:
        try:
            print("Starting: PyFuse Main...")
            nursery.start_soon(pyfuse3.main)

            print("Starting: Persist...")        
            nursery.start_soon(persist, stat_msg_queue)

            print("Start: Usage...")
            nursery.start_soon(usage, stat_msg_queue)
            
        except:
            print(traceback.format_exc())


def write_small_block_to_disk(swq, stat_msg_queue):
    while True:
        try:
            if not swq.empty():
                start = time.time()
                d_hash, d_data = swq.get(False)                
                written = write_small_block(d_hash, bytearray(d_data), stat_msg_queue)                
                del d_data
                del d_hash
                if random.randrange(0,10) == 0:
                    stat_msg_queue.put_nowait({'INFO:WriteSpeed' : written/(time.time()-start)})                
        except:
            continue


def write_to_disk(wq, resq, stat_msg_queue):

    while True:
        written = 0
        bytes_written = 0
        start = time.time()        
        if not wq.empty():
            try:
                q, datastore = wq.get(False)                
                
                with open(datastore[q.chunk].path, "r+b") as f:
                    mm = mmap.mmap(f.fileno(), length=datastore[q.chunk].size, access=mmap.ACCESS_WRITE)
                    # for q, datastore in work:                    
                    mm.seek(q.block)
                    if q.compressed:                            
                        written = mm.write(q.compressed_data)                        
                    else:                            
                        written = mm.write(q.data)                        
                    # mm.madvise(mmap.MADV_DONTNEED)                    
                    mm.close()
                    del mm
                    # gc.collect()
                    # del q
                    # del datastore                    
                                    
            except ValueError:
                print("ValueError Writing data to the disk: Chunk:{}, Block:{}, Data Size:{}".format(q.chunk, q.block, len(q.compressed_data)))
                print(traceback.format_exc())
            except queue.Empty:
                continue
            if random.randrange(0,10) == 0:
                stat_msg_queue.put_nowait({'INFO:WriteSpeed' : written/(time.time()-start)})
            
            # if unix_memory() > writer_thread_mem_limit:
            #     break
        else:
            del written
            del bytes_written
            continue
    

async def persist(stat_msg_queue):

    # ctx = mp.get_context('fork')
        
    wq = mp.Queue() # Fila de mensagens a serem escritas no disco
    resq = mp.Queue() # Fila com as respostas das mensagens escritas
    swq = mp.Queue() # Fila de escrita de blocos pequenos, estes nao tem fila de retorno
    
    smbw_threads = []
    bbw_threads = []

    for i in range(0, smb_write_threads):
        p = mp.Process(target=write_small_block_to_disk, args=(swq,stat_msg_queue ))
        p.start()
        smbw_threads.append(p)

    for i in range(0, bb_write_threads):
        p = mp.Process(target=write_to_disk, args=(wq,resq,stat_msg_queue, ))
        p.start()
        bbw_threads.append(p)
   
    time.sleep(5)
    last_time = time.time()    
    while True:
        if (time.time() - last_time > gc_interval or len(write_buffer) > write_buffer_size) and not write_buffer_lock:    
            if len(write_buffer) > 0:                
                await trio.to_thread.run_sync(write_new_blocks, write_buffer, wq, resq, swq, stat_msg_queue)
            await trio.to_thread.run_sync(garbage_collector, stat_msg_queue)
            await trio.to_thread.run_sync(persist_data, [inodes, contents], stat_msg_queue)
            last_time = time.time()            

            for i, t in enumerate(smbw_threads):
                try:
                    if not t.is_alive():
                        try:
                            th = smbw_threads.pop(i)
                            th.terminate()
                            smbw_threads.append(mp.Process(target=write_small_block_to_disk, args=(swq,stat_msg_queue )).start())
                        except:
                            smbw_threads.append(mp.Process(target=write_small_block_to_disk, args=(swq,stat_msg_queue )).start())
                    
                except AttributeError:
                    th = smbw_threads.pop(i)
                    smbw_threads.append(mp.Process(target=write_small_block_to_disk, args=(swq,stat_msg_queue )).start())

            for i, t in enumerate(bbw_threads):
                try:
                    if not t.is_alive():
                        try:
                            th = bbw_threads.pop(i)
                            th.terminate()
                            bbw_threads.append(mp.Process(target=write_to_disk, args=(wq,resq,stat_msg_queue, )).start())
                        except:
                            bbw_threads.append(mp.Process(target=write_to_disk, args=(wq,resq,stat_msg_queue, )).start())                            
                    
                except AttributeError:
                    th = bbw_threads.pop(i)
                    bbw_threads.append(mp.Process(target=write_to_disk, args=(wq,resq,stat_msg_queue, )).start())


        gc.collect()
        await trio.sleep(0.1)


    for t in smbw_threads:
        try:
            t.join(timeout=5)
            t.kiil()
        except:
            continue

    for t in bbw_threads:
        try:
            t.join(timeout=5)
            t.kiil()
        except:
            continue    

    
async def usage(stat_msg_queue):
    mem = virtual_memory()

    time.sleep(30)
    last_time = time.time()    
    while True:        
        if (time.time() - last_time > usage_interval):  
            await trio.to_thread.run_sync(get_usage, stat_msg_queue)
            stat_msg_queue.put({'INFO:TOTAL_MEMORY': mem})
            stat_msg_queue.put({'INFO:Memory (active in use)': unix_memory()})
            stat_msg_queue.put({'INFO:Memory (resident)': resident()})
            stat_msg_queue.put({'INFO:Memory (stack size)': stacksize()})
            last_time = time.time()
        await trio.sleep(30)


'''

MAIN PROGRAM

'''

if __name__ == '__main__':

    from functools import partial

    datastore, free_blocks, key_index, hash_table, fs_meta, GC = init_persistance()

    stat_msg_queue = mp.Queue()
    
    stat_sender = mp.Process(target=mq_client.send_message, args=(stat_msg_queue, ))
    stat_sender.start()

    options = parse_args()
    # init_logging(options.debug)
    operations = Operations(stat_msg_queue)

    try:
        os.system("fusermount -u "+options.mountpoint)
    except:
        pass

    fuse_options = set(pyfuse3.default_options)
    fuse_options.add('fsname=VeratyFS')
    fuse_options.discard('default_permissions')
    # if options.debug_fuse:
    #     fuse_options.add('debug')    
    pyfuse3.init(operations, options.mountpoint, fuse_options)
    
    try:
        par = partial(parent, stat_msg_queue=stat_msg_queue)
        trio.run(par)    
    except:
        pyfuse3.close(unmount=False)
        print(traceback.format_exc())
        print("TODO: REDO THAT FOR MP - Persisting remaining data...")
        # if len(write_buffer) > 0 and not write_buffer_lock:
        #     pass
        # persist_data([inodes, contents], stat_msg_queue)
        
        # os.system("fusermount -u "+options.mountpoint)
        stat_sender.join(2)
        stat_sender.kill()
    finally:
        pyfuse3.close()