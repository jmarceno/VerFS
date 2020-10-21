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

# import debugpy
# debugpy.debug_this_thread()
import os
import sys

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

from datastructures import *
from configurations import *
from persistence import init_persistance, persist_data
from stats import unix_memory, resident, stacksize

# from tqdm.asyncio import trange, tqdm
import tqdm

datastore, free_blocks, key_index, hash_table, fs_meta, GC = init_persistance()

global_data = {'datastore':datastore, 'free_blocks':free_blocks, 'key_index':key_index, 'hash_table':hash_table, 'fs_meta': fs_meta, 'GC': GC}
write_buffer_lock = False

from psutil import virtual_memory
mem = virtual_memory()

format_ =  '{l_bar}{bar}{r_bar}'], where l_bar='{desc}: {percentage:3.0f}%|' and r_bar='| {n_fmt}/{total_fmt} [{elapsed}<{remaining}, ' '{rate_fmt}{postfix}]

space_bar_used = tqdm.tqdm(total=partition_size_gb, leave=True, unit=' Bytes', colour='green', mininterval=5,)
space_bar_used.set_description("Used Space")
space_bar_savings = tqdm.tqdm(total=partition_size_gb, leave=True, unit=' Bytes', colour='green', mininterval=5)
space_bar_savings.set_description("Saved Space with (Comp+Dedup):")
space_bar_compression_rate = tqdm.tqdm(total=1, leave=True, unit=' %', colour='green', mininterval=5)
space_bar_compression_rate.set_description("Compression Rate")
space_bar_free_space = tqdm.tqdm(total=partition_size_gb, leave=True, unit=' Bytes', colour='green', mininterval=5)
space_bar_free_space.set_description("Free Space")

memory_bar_active = tqdm.tqdm(total=mem.total, leave=True, unit=' Bytes', colour='cyan', mininterval=5)
memory_bar_active.set_description("Used memory - Active")
memory_bar_inactive = tqdm.tqdm(total=mem.total, leave=True, unit=' Bytes', colour='cyan', mininterval=5)
memory_bar_inactive.set_description('Used memory - Resident')
memory_bar_statck = tqdm.tqdm(total=mem.total, leave=True, unit=' Bytes', colour='cyan', mininterval=5)
memory_bar_statck.set_description("Used memory - Stack")

write_bar = tqdm.tqdm(total=1, leave=True, unit=' Blocks', colour='red',miniters=0)
write_bar.set_description("Disk writing (flush)")
dedup_bar = tqdm.tqdm(total=1, leave=True, unit=' Blocks', colour='magenta', miniters=0)
dedup_bar.set_description("Deduplicating data")

try:
    if global_data['fs_meta'][0] is not None:
        inodes = global_data['fs_meta'][0]
        contents = global_data['fs_meta'][1]
    else:
        inodes = inodes
        contents = contents
except:
        inodes = inodes
        contents = contents

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
    global global_data

    enable_writeback_cache = True    

    def __init__(self):
        super(Operations, self).__init__()
        
        self.inode_open_count = defaultdict(int)
        try:
            if global_data['fs_meta'] is not None and global_data['fs_meta'][0] is None or global_data['fs_meta'][1] is None:
                self.inodes = inodes
                self.contents = contents
                self.init_file_system()
            else:
                self.inodes = global_data['fs_meta'][0]
                self.contents = global_data['fs_meta'][1]
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
                    if self.contents[i].parent_inode ==  inode_p and self.contents[i].name == name:
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

        return entry


    async def readlink(self, inode, ctx):
        return self.inodes[inode].target        

    async def opendir(self, inode, ctx):
        return inode

    async def readdir(self, inode, off, token):
        dir_entries = []
        for d in self.contents:
            if self.contents[d].parent_inode == inode:
                dir_entries.append(self.contents[d])

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
            removed = self.inodes.pop(entry.st_ino)
            for b in removed.data:
                update_index(b.hash, chunk=None, add=False)

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
        stat_.f_blocks = partition_size_gb // stat_.f_frsize    
        # stat_.f_blocks = size // stat_.f_frsize
        stat_.f_bfree = (partition_size_gb - get_usage()[2]) // allocation_unit #stat_.f_blocks - (size // allocation_unit)
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
        return pyfuse3.FileInfo(fh=inode)

    async def access(self, inode, mode, ctx):
        # Yeah, could be a function and has unused arguments
        #pylint: disable=R0201,W0613
        return True

    async def create(self, inode_parent, name, mode, flags, ctx):
        #pylint: disable=W0612
        entry = await self._create(inode_parent, name, mode, ctx)
        self.inode_open_count[entry.st_ino] += 1
        return (pyfuse3.FileInfo(fh=entry.st_ino), entry)

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
        # debugpy.debug_this_thread()
        f = None
        for i in list(self.inodes.keys()):
            if self.inodes[i].id == fh:
                f = self.inodes[i]

        if len(self.inodes[fh].data) == 0:
            data = b''
        
        else:        
            if offset >= self.inodes[fh].size:
                print("End of File")
                raise IOError
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
                return get_file_data(self.inodes[fh].data, start_blk, end_blk + 1)[offset:end_offset]
            else:
                data = get_file_data(self.inodes[fh].data, start_blk, end_blk)

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

            if data == b'':
                print("Fuck it")

            if len(data) != (end_offset - offset):
                print("data problem")
            return data

        if data is None:
            data = b''

        return data

    async def write(self, fh, offset, buf):
        # debugpy.debug_this_thread()
        f = None
        for i in list(self.inodes.keys()):
            if self.inodes.get(i).id == fh:
                f = self.inodes[i]

        end_offset = offset + len(buf)
        if end_offset > f.size:
            f.size = end_offset #TODO: SETAR TAMANHO DO ARQUIVO DE FORMA CORRETA

        f.data += dedup(bytes(buf))
        
        self.inodes[fh].data = f.data
        self.inodes[fh].size = f.size
        # self.inodes.update({fh, f})       
        
        inodes = self.inodes    # Coloca os dados do FS de volta na memória compartilhada para pode ser acessada e persistida
        contents = self.contents    #TODO: TROCAR A FORMA DE USO PARA NAO PRECISAR MANTER 2 VARIAVEIS NA MEMORIA

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

    space_bar_used.reset()
    space_bar_used.n = deduped_compressed
    space_bar_used.refresh()

    space_bar_savings.reset()
    space_bar_savings.n = (undeduped_uncompressed - undeduped_compressed)+(undeduped_uncompressed - deduped_compressed)
    space_bar_savings.refresh()

    compbar = (1 - compression_rate)
    if compbar == 1:
        compbar = 0

    space_bar_compression_rate.reset()
    space_bar_compression_rate.n = (1 - compression_rate)
    space_bar_compression_rate.refresh()

    space_bar_free_space.reset()
    space_bar_free_space.n = partition_size_gb - deduped_compressed
    space_bar_free_space.refresh()
    # print("Undeduped (No Compression) Space Used : " + humanbytes(undeduped_uncompressed))
    # print("Undeduped (Compression) Space Used : " + humanbytes(undeduped_compressed))
    # print("Deduped (Compression) Space Used : " + humanbytes(deduped_compressed) + " [Duplicated data found (Saved Space): " + humanbytes(undeduped_uncompressed -deduped_compressed) + " ]")
    # print("Compression Rate : " + str(1 - compression_rate) + "% Saved Space: " + humanbytes(undeduped_uncompressed - undeduped_compressed))
    # print("Total Savings: " + humanbytes((undeduped_uncompressed - undeduped_compressed)+(undeduped_uncompressed -deduped_compressed)))

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
    
    write_buffer_lock = True
    registers_processed = 0
    bytes_processed = 0
    writing = True    
    start_time = time.time()

    write_bar.reset()    
    write_bar.total = len(write_buffer)
    write_bar.refresh()
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
                                    # mm.flush()
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
                        
                        write_bar.update(1)
                        # write_bar.refresh()
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
            # if registers_processed > 0:
            #     print("DEBUG: Write Queue has been processed. " + str(registers_processed) + " registers")
            #     print("DEBUG: Processed -> "+humanbytes(bytes_processed)+" in "+humanbytes(bytes_processed/(time.time()-start_time))+" /s")                
                # persist_data(fs_meta)                
            writing = False
            write_buffer_lock = False                
        finally:
            pass
            # write_bar.set_postfix(Speed=humanbytes(bytes_processed/(time.time()-start_time))+ "/s" , refresh=False)
            # write_bar.update(1)
            # write_bar.refresh()
            # pbar.update(1)
            # break
    write_buffer_lock = False
    if registers_processed > 0:
        pass
        # print("DEBUG: Write Queue has been processed. " + str(registers_processed) + " registers")
        # print("DEBUG: Processed -> "+humanbytes(bytes_processed)+" in "+humanbytes(bytes_processed/(time.time()-start_time))+" /s")    


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
        # dedup_bar.reset()
        dedup_bar.total = len(ch) + dedup_bar.total
        dedup_bar.refresh()
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
            dedup_bar.update(1)
            # dedup_bar.refresh()
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

class NoUniqueValueError(Exception):
    def __str__(self):
        return 'Query generated more than 1 result row'


class NoSuchRowError(Exception):
    def __str__(self):
        return 'Query produced 0 result rows'

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


async def parent():        
    print("parent: started!")    
    async with trio.open_nursery() as nursery:
        print("parent: PyFuse Main...")
        nursery.start_soon(pyfuse3.main)

        print("parent: Persist...")
        nursery.start_soon(persist)

        print("parent: Usage...")
        nursery.start_soon(usage)

        print("parent: waiting for children to finish...")
        os.system('clear')
        # -- we exit the nursery block here --
    print("parent: all done!")


async def persist():
    last_time = time.time()
    while True:
        if time.time() - last_time > gc_interval and not write_buffer_lock:
            # print('======================')
            # print("DEBUG: Starting Write")
            if len(write_buffer) > 0:
                write_new_blocks(write_buffer)
                garbage_collector()
                persist_data([inodes, contents])
            last_time = time.time()
            # print('======================')
        await trio.sleep(1)


async def usage():
    last_time = time.time()
    while True:
        if time.time() - last_time > gc_interval:
            # print('======================')
            # print("DEBUG: Usage")
            get_usage()
            # print('======================')
            memory_bar_active.reset()
            memory_bar_active.n = unix_memory()
            memory_bar_active.refresh()
            
            memory_bar_inactive.reset()
            memory_bar_inactive.n = resident()
            memory_bar_inactive.refresh()
            
            memory_bar_statck.reset()
            memory_bar_statck.n = stacksize()
            memory_bar_statck.refresh()
            # tqdm.tqdm.write("Memory Used: "+ humanbytes(unix_memory()))
            # tqdm.tqdm.write("Memory Stack Size: "+ humanbytes(stacksize()))
            # tqdm.tqdm.write("Resident Memory: "+ humanbytes(resident()))
            # print("Memory Used: "+ humanbytes(unix_memory()))
            # print("Resident Memory: "+humanbytes(resident()))
            # print("Memory Stack Size: "+humanbytes(stacksize()))
            # print('======================')
            last_time = time.time()
        await trio.sleep(5)


'''

MAIN PROGRAM

'''

if __name__ == '__main__':    
    options = parse_args()
    init_logging(options.debug)
    operations = Operations()

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
        trio.run(parent)
    except:
        pyfuse3.close(unmount=False)        
        raise

    pyfuse3.close()