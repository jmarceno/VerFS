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

from errno import ENOSPC
import os
from random import randint
from storage.rocksdb_backend import rocksdb_delete
import sys
import multiprocessing as mp
import random
# # If we are running from the pyfuse3 source directory, try
# # to load the module from there first.
# basedir = os.path.abspath(os.path.join(os.path.dirname(sys.argv[0]), '..'))
# if (os.path.exists(os.path.join(basedir, 'setup.py')) and
#     os.path.exists(os.path.join(basedir, 'src', 'pyfuse3.pyx'))):
#     sys.path.insert(0, os.path.join(basedir, 'src'))
import gc
import time
import copy
import errno
import stat
import mmap
import traceback
import threading
from time import time
import logging
from collections import OrderedDict, deque
import pyfuse3
from BTrees import IOBTree
from collections import defaultdict
from pyfuse3 import FUSEError
from argparse import ArgumentParser
import trio
from psutil import virtual_memory

import mq_client
from hashing import hashed_chunks, hash_data
from stats import Timer, humanbytes, memory
from compression import decompress_data, compress_data
from logger import LogEvent
from datastructures import *
from configurations import *
from smbs import write_small_block, read_small_block, delete_small_block
from persistence import init_persistance, persist_data
from stats import unix_memory, resident, stacksize
from utils import take_closest
from storage.storage_backend import commit_data, read_data
from fsmeta import FSMeta
# from hashtable import HashTable
# from keyindex import KeyIndex


# import builtins
# import line_profiler
# prof = line_profiler.LineProfiler()
# builtins.__dict__['profile'] = prof

try:
    import faulthandler
except ImportError:
    pass
else:
    faulthandler.enable()

log = logging.getLogger()

# if replicate_data and (replication_type == 'async' or replication_type == 'batch'):
#     mirror_queue = mp.Queue()

'''
Global variables for statistics and state monitoring
No persistance is necessary
'''
cache_hits = 0
cache_misses = 0

'''
Maintain track of the program state. When it is set to false, the auxiliary/service threads are all finish the execution
'''
RUNNING = True
# Indicates that the write_buffer is full and writes are not being processed anymore (See @dedup)
STALLED = False
# An empty buffer that will replace the write_buffer after its exaustion in consequence of a stall
TEMP_WRITE_BUFFER = deque()

class Operations(pyfuse3.Operations):
    '''
    File system operations
    Main loop responsible for receiving the requestes from the kernel and 
    process the data
    '''    
    
    enable_writeback_cache = False
    
    def __init__(self, stat_msg_queue):
        super(Operations, self).__init__()
        
        self.locked = False  # Write lock to prevent to many write threads to wait in line
        
        self.inode_open_count = defaultdict(int)
        self.stat_msg_queue = stat_msg_queue  
        self.inodes = FSMeta()
                
        if len(self.inodes) == 0:
            self.init_file_system()
        
        self.services = []
        p = threading.Thread(target=persist, args=(self.stat_msg_queue,), daemon=True)
        u = threading.Thread(target=usage, args=(self.stat_msg_queue,), daemon=True)
        p.start()
        u.start()
        self.services.append(p)
        self.services.append(u)
        
        self.writers = []

        for i in range(0, max_write_workers):
            t = threading.Thread(target=write_new_blocks, args=(self.stat_msg_queue, True), daemon=True)
            t.start()
            self.writers.append(t)

        # if replicate_data and (replication_type == 'async' or replication_type == 'batch'):
        #     self.services.append(mp.Process(target=, args=(self.stat_msg_queue, )))

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
        new_file.name = b'..'
        new_file.parent_inode = pyfuse3.ROOT_INODE
        new_file.inode = pyfuse3.ROOT_INODE

        self.inodes[pyfuse3.ROOT_INODE] = new_file
        

    async def lookup(self, inode_p, name, ctx=None):
        
        inode = None
        if name == '.':
            inode = inode_p
        elif name == '..':
            inode = self.inodes[inode_p]
        else:
            try:
                for x in self.inodes:
                    if self.inodes[x].parent_inode==inode_p and self.inodes[x].name==name and self.inodes[x].list_on_dir_lookup:
                        inode = self.inodes[x].inode
                        self.inodes[x].lookup_count = self.inodes[x].lookup_count + 1
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

            entry.st_nlink = f.st_nlink

            entry.st_uid = f.uid
            entry.st_gid = f.gid
            entry.st_rdev = f.rdev
            entry.st_size = f.size

            entry.st_blksize = allocation_unit
            entry.st_blocks = 1
            entry.st_atime_ns = f.atime_ns
            entry.st_mtime_ns = f.mtime_ns
            entry.st_ctime_ns = f.ctime_ns
           
        except KeyError:
            raise (pyfuse3.FUSEError(errno.ENOENT))

        return entry


    async def readlink(self, inode, ctx):
        return self.inodes[inode].target 
               

    async def opendir(self, inode, ctx):
        return inode

    #@profile
    async def readdir(self, inode, off, token):
        dir_entries = []
        [dir_entries.append(self.inodes[x]) for y,x in enumerate(self.inodes, off) if self.inodes[x].parent_inode==inode and self.inodes[x].list_on_dir_lookup]
        
        try:
            pyfuse3.readdir_reply(token, dir_entries[off].name, await self.getattr(dir_entries[off].inode), off+1)
            
        except IndexError:
            return False


    async def unlink(self, inode_p, name, ctx):
        
        
        entry = await self.lookup(inode_p, name)
        
        if stat.S_ISDIR(entry.st_mode):
            raise pyfuse3.FUSEError(errno.EISDIR)        
        
        i = self.inodes[entry.st_ino]
        i.lookup_count = i.lookup_count - 1        
        i.st_nlink = i.st_nlink - 1        
        i.list_on_dir_lookup = False

        self.inodes[entry.st_ino] = i

        # if i.st_nlink <= 0:
        #     self._remove(inode_p, name, await self.getattr(i.inode, ctx))

        await self.inodes.commit()
                

    async def forget(self, inode_list):
        '''Decrease lookup counts for inodes in *inode_list*
        *inode_list* is a list of ``(inode, nlookup)`` '''
        
        
        for n in inode_list:
            try:
                if self.inodes[n[0]].inode != pyfuse3.ROOT_INODE:      
                    i = self.inodes[n[0]]
                    i.lookup_count = i.lookup_count - n[1]
                    self.inodes[n[0]] = i
                    if self.inodes[n[0]].st_nlink <= 0:                        
                        pyfuse3.invalidate_entry_async(self.inodes[n[0]].parent_inode, self.inodes[n[0]].name, deleted=0, ignore_enoent=True)
                        self._remove(self.inodes[n[0]].parent_inode, self.inodes[n[0]].name, await self.getattr(n[0]))
                        
                else:
                    i = self.inodes[n[0]]
                    i.lookup_count = 1
                    self.inodes[n[0]] = i
            except:                
                pass
        await self.inodes.commit()
        
        # self.lock.release()


    async def rmdir(self, inode_p, name, ctx):
        entry = await self.lookup(inode_p, name)

        if not stat.S_ISDIR(entry.st_mode):
            raise pyfuse3.FUSEError(errno.ENOTDIR)

        for i in self.inodes:
            if self.inodes[i].parent_inode == entry.st_ino and self.inodes[i].list_on_dir_lookup == True:
                raise FUSEError(errno.ENOTEMPTY)       
        
        pyfuse3.invalidate_entry_async(self.inodes[entry.st_ino].parent_inode, self.inodes[entry.st_ino].name, deleted=0, ignore_enoent=True)
        del self.inodes[entry.st_ino]        

        await self.inodes.commit()
        

    def _remove(self, inode_p, name, entry, d=False):
        if self.inodes[inode_p].inode != inode_p and self.inodes[inode_p].parent_inode == inode_p and self.inodes[inode_p].name != name:
            raise pyfuse3.FUSEError(errno.ENOTEMPTY)

        # for k, v in self.inodes.items():
        #     if v.name == name and v.parent_inode == inode_p:
        try:
            f = self.inodes[entry.st_ino].data
            for i in f:
                update_index(i.hash, add=False, stat_msg_queue=self.stat_msg_queue)
            pyfuse3.invalidate_entry_async(self.inodes[entry.st_ino].parent_inode, self.inodes[entry.st_ino].name, deleted=0, ignore_enoent=True)
            del self.inodes[entry.st_ino]
        except KeyError:
            LogEvent(("ERROR",traceback.format_exc()))


    async def symlink(self, inode_p, name, target, ctx):
        mode = (stat.S_IFLNK | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR |
                stat.S_IRGRP | stat.S_IWGRP | stat.S_IXGRP |
                stat.S_IROTH | stat.S_IWOTH | stat.S_IXOTH)
        
        entry = await self._create(inode_p, name, mode, ctx, target=target)

        i = self.inodes[entry.st_ino]
        i.lookup_count = i.lookup_count + 1
        self.inodes[entry.st_ino] = i

        await self.inodes.commit()        

        return entry


    async def rename(self, inode_p_old, name_old, inode_p_new, name_new, flags, ctx):
        '''
        Rename a directory entry.

        This method must rename *name_old* in the directory with inode
        *parent_inode_old* to *name_new* in the directory with inode
        *parent_inode_new*.  If *name_new* already exists, it should be
        overwritten.

        *flags* may be `RENAME_EXCHANGE` or `RENAME_NOREPLACE`. If
        `RENAME_NOREPLACE` is specified, the filesystem must not overwrite
        *name_new* if it exists and return an error instead. If
        `RENAME_EXCHANGE` is specified, the filesystem must atomically exchange
        the two files, i.e. both must exist and neither may be deleted.

        *ctx* will be a `RequestContext` instance.
        '''
        
        if flags != 0:
            raise FUSEError(errno.EINVAL)

        entry_old = await self.lookup(inode_p_old, name_old)
        
        
        try:
            entry_new = await self.lookup(inode_p_new, name_new)
        except pyfuse3.FUSEError as exc:
            if exc.errno != errno.ENOENT:
                raise
        
            old = self.inodes[entry_old.st_ino]            
            old.name = name_new
            old.parent_inode = inode_p_new
            self.inodes[entry_old.st_ino] = old
        else:            
            self._replace(inode_p_old, name_old, inode_p_new, name_new, entry_old, entry_new)
            
        
        await self.inodes.commit()
        
            

    def _replace(self, inode_p_old, name_old, inode_p_new, name_new, entry_old, entry_new):
        '''Let the inode associated with *name_old* in *parent_inode_old* be
        *inode_moved*, and the inode associated with *name_new* in
        *parent_inode_new* (if it exists) be called *inode_deref*.

        If *inode_deref* exists and has a non-zero lookup count, or if there are
        other directory entries referring to *inode_deref*), the file system
        must update only the directory entry for *name_new* to point to
        *inode_moved* instead of *inode_deref*.'''

        inode_moved = self.inodes[entry_old.st_ino]
        inode_deref = self.inodes[entry_new.st_ino]
        try:            
            if inode_deref.lookup_count > 0:
                inode_moved.name = name_new
                inode_moved.parent_inode = inode_p_new                
                self.inodes[entry_old.st_ino] = inode_moved #<- Point the new back to the old?
                inode_deref.list_on_dir_lookup = False
                inode_deref.st_nlink = inode_deref.st_nlink - 1
                self.inodes[entry_new.st_ino] = inode_deref
        
                return True
            else:
                inode_moved.name = name_new
                inode_moved.parent_inode = inode_p_new                
                self.inodes[entry_old.st_ino] = inode_moved
                pyfuse3.invalidate_entry_async(self.inodes[entry_new.st_ino].parent_inode, self.inodes[entry_new.st_ino].name, deleted=0, ignore_enoent=True)
                del self.inodes[entry_new.st_ino]
        except:
            LogEvent(("ERROR",traceback.format_exc()))
            pass
        
        # inode_moved.name = name_new
        # inode_moved.parent_inode = inode_p_new
        # del self.inodes[inode_moved.inode]
        # self.inodes[inode_moved.inode] = inode_moved        
        self.inodes.commit()
        return True
        

    def gen_inode_number(self):        
        return self.inodes.max() + 1


    async def link(self, inode, new_inode_p, new_name, ctx):
        entry_p = await self.getattr(new_inode_p)
        if entry_p.st_nlink == 0:
            LogEvent(("ERROR",'Attempted to create entry '+ str(new_name) + 'with unlinked parent '+ str(new_inode_p)))
            # print('Attempted to create entry '+ str(new_name) + 'with unlinked parent '+ str(new_inode_p))
            raise FUSEError(errno.EINVAL)
                
        ni = File_Inode(self.gen_inode_number())
        ni.name = new_name
        ni.parent_inode = new_inode_p
        ni.lookup_count = 1
        ni.hard_link = inode
        self.inodes[ni.inode] = ni
                
        i = self.inodes[inode]
        i.st_nlink = i.st_nlink + 1
        self.inodes[inode] = i
        
        await self.inodes.commit()

        return await self.getattr(inode)
        
    '''
    Truncates the file *down* to a **new_size**
    This is a slow operation as it needs to read the file content, apply it to a temporary file
    then replace the old file metadata with the new one and *free* the remaining blocks
    '''
    async def truncate_down(self, fh, new_size):
        f_new = copy.copy(self.inodes[fh])
        
        f_new.data = []
        f_new.offsets = []
        f_new.size = 0

        temp_ino = self.gen_inode_number()
        self.inodes[temp_ino] = f_new
        
        try:
            offset = 0
            for i in self.inodes[fh].data:
                if i.raw_size + offset <= new_size:
                    dat = await self.read(fh, offset, i.raw_size)
                    written = await self.write(temp_ino, offset, dat)        
                    offset = offset + written
                    if offset == new_size:
                        break
                else:
                    dat = await self.read(fh, offset, new_size - offset)
                    written = await self.write(temp_ino, offset, dat)                
                    break

            remove = list(set(self.inodes[fh].data) - set(self.inodes[temp_ino].data))
                    
            for i in remove:
                update_index(i.hash, add=False)
                
            
            self.inodes[fh] = self.inodes[temp_ino]        
            del self.inodes[temp_ino]

        except:
            print(traceback.format_exc)
            LogEvent(("ERROR",traceback.format_exc()))
                        

    '''Change attributes of *inode*

    *fields* will be an `SetattrFields` instance that specifies which
    attributes are to be updated. *attr* will be an `EntryAttributes`
    instance for *inode* that contains the new values for changed
    attributes, and undefined values for all other attributes.

    Most file systems will additionally set the
    `~EntryAttributes.st_ctime_ns` attribute to the current time (to
    indicate that the inode metadata was changed).

    If the syscall that is being processed received a file descriptor
    argument (like e.g. :manpage:`ftruncate(2)` or :manpage:`fchmod(2)`),
    *fh* will be the file handle returned by the corresponding call to the
    `open` handler. If the syscall was path based (like
    e.g. :manpage:`truncate(2)` or :manpage:`chmod(2)`), *fh* will be
    `None`.

    *ctx* will be a `RequestContext` instance.

    The method should return an `EntryAttributes` instance (containing both
    the changed and unchanged values).
    '''
    async def setattr(self, inode, attr, fields, fh, ctx):

        old_inode = self.inodes[inode]        

        if fields.update_size:            
            try:
                if old_inode.size < attr.st_size:
                    await self.write(old_inode.inode, old_inode.size, (bytearray(b'\x00') *(attr.st_size-old_inode.size)))
                elif old_inode.size > attr.st_size:                    
                    await self.truncate_down(old_inode.inode, attr.st_size)
                old_inode = self.inodes[inode]                
            except:
                print(traceback.format_exc())
                LogEvent(("ERROR",traceback.format_exc()))
            
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


    '''
    unsigned long  f_bsize;    /* Filesystem block size */
    unsigned long  f_frsize;   /* Fragment size */
    fsblkcnt_t     f_blocks;   /* Size of fs in f_frsize units */
    fsblkcnt_t     f_bfree;    /* Number of free blocks */
    fsblkcnt_t     f_bavail;   /* Number of free blocks for
                                    unprivileged users */
    fsfilcnt_t     f_files;    /* Number of inodes */
    fsfilcnt_t     f_ffree;    /* Number of free inodes */
    fsfilcnt_t     f_favail;   /* Number of free inodes for
                                    unprivileged users */
    unsigned long  f_fsid;     /* Filesystem ID */
    unsigned long  f_flag;     /* Mount flags */
    unsigned long  f_namemax;  /* Maximum filename length */
    '''
    async def statfs(self, ctx):      

        stat_ = pyfuse3.StatvfsData()

        stat_.f_bsize = allocation_unit
        stat_.f_frsize = allocation_unit

        size = 0
        for k in list(self.inodes.keys()):
            size = size + self.inodes[k].size
        stat_.f_blocks = max(0, (partition_size_gb // allocation_unit))
        # stat_.f_blocks = size // stat_.f_frsize
        stat_.f_bfree = max(0, (partition_size_gb - get_usage(self.stat_msg_queue)[2]) // allocation_unit )#stat_.f_blocks - (size // allocation_unit)
        # stat_.f_bfree = max(size // stat_.f_frsize, 1024)
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
        return pyfuse3.FileInfo(fh=inode, direct_io=False)


    async def access(self, inode, mode, ctx):
        # Yeah, could be a function and has unused arguments
        #pylint: disable=R0201,W0613
        return True


    async def create(self, inode_parent, name, mode, flags, ctx):
        # await self.lock.acquire()
        #pylint: disable=W0612
        entry = await self._create(inode_parent, name, mode, ctx)
        self.inode_open_count[entry.st_ino] += 1
        # self.lock.release()
        return (pyfuse3.FileInfo(fh=entry.st_ino, direct_io=False), entry)


    async def _create(self, inode_p, name, mode, ctx, rdev=0, target=None):        

        if (await self.getattr(inode_p)).st_nlink == 0:
            LogEvent(("ERROR",'Attempted to create entry '+ str(name) + 'with unlinked parent '+ str(inode_p)))
            raise FUSEError(errno.EINVAL)
        
        now_ns = time.time_ns()        
        new_file = File_Inode(self.gen_inode_number())
        new_file.mode = mode
        new_file.uid = ctx.uid
        new_file.gid = ctx.gid
        new_file.mtime_ns = now_ns
        new_file.atime_ns = now_ns
        new_file.ctime_ns = now_ns
        new_file.rdev = rdev
        new_file.name = name
        new_file.parent_inode = inode_p
        if target is not None:
            new_file.target = target
        
        self.inodes[new_file.inode] = new_file
        await self.inodes.commit()

        return await self.getattr(new_file.inode)


    async def retrieve_data(self, fh, offset, length, whole=False):        
        try:
            l = len(self.inodes[fh].data)
        except:
            return b''

        if l == 0:
            data = b''
        
        else:                    
            end_offset = min(self.inodes[fh].size, offset + length)

            start_blk = 0
            start_diff = 0
            end_blk = 0
            end_diff = 0                        
               
            if len(self.inodes[fh].offsets) == 0:
                await self.calc_offsets(fh)                
                await self.inodes.commit()
            
            blk, blk_number = take_closest(self.inodes[fh].offsets, offset, left=True)            
            if offset != 0:
                if blk == offset:
                    start_blk = blk_number + 1
                else:
                    if blk - offset < 0:
                        start_blk = blk_number - 1
                        start_diff = self.inodes[fh].offsets[start_blk] - offset
                    else:
                        start_blk = blk_number
                        start_diff = blk - offset
            
            blk_e, blk_number_e = take_closest(self.inodes[fh].offsets, end_offset, left=True)                        
            if blk_e == end_offset:
                end_blk = blk_number_e
            else:
                end_diff = blk_e - end_offset
                end_blk = blk_number_e

            if start_blk > end_blk:
                LogEvent(("ERROR","Error reading data"))

            if start_blk == 0 and end_blk == 0:
                if not whole:                    
                    data = get_file_data(self.stat_msg_queue, self.inodes[fh].data, start_blk, end_blk + 1)
                    data = data[offset:end_offset]
                else:
                    if start_diff < 0:
                        LogEvent(("ERROR","Negative START_DIFF"))                        
                    data = get_file_data(self.stat_msg_queue, self.inodes[fh].data, start_blk, end_blk + 1)
                    
                    return start_diff, start_blk, end_blk, data
            else:
                data = get_file_data(self.stat_msg_queue, self.inodes[fh].data, start_blk, end_blk)
                backupdata = data # TODO: REMOVE for production

                if whole:
                    if start_diff < 0:
                        start_blk = max(0, start_blk -1)
                    return start_diff, start_blk, end_blk, data

                if self.inodes[fh].size == end_offset:
                    data = data[-(end_offset-offset):]
                else:
                    if start_diff < 0:
                        LogEvent(("ERROR","Negative START_DIFF"))
                    if start_blk == 0:
                        data = data[offset:]
                    elif offset > 0 and start_diff > 0:
                        data = data[self.inodes[fh].data[start_blk].raw_size-start_diff:]
                    if end_diff != 0 and len(data) > (end_offset-offset):
                        data = data[:-end_diff]
            

            if len(data) != (end_offset - offset):
                LogEvent(("ERROR","Returning wrong length data @ [async def retrieve data]. Is this intended?"))                
                return data

        if data is None:
            data = b''
        
        await trio.sleep(0) # Adds a checkpoint so other co-routines have a chance to run
        
        return data

    
    async def read(self, fh, offset, length, whole=False): 
        start_time = time.time()       
        
        data = await self.retrieve_data(fh, offset, length, whole=False)
        self.stat_msg_queue.put({'INFO:ReadSpeed' : str(len(data)/ (time.time()- start_time))})

        inode = self.inodes[fh]
        inode.atime_ns = time.time_ns()
        self.inodes[fh] = inode
        # await self.inodes.commit()

        return data

    
    # async def fsync(self, fh, datasync):
    #     '''Flush buffers for open file *fh*

    #     If *datasync* is true, only the file contents should be
    #     flushed (in contrast to the metadata about the file).

    #     *fh* will by an integer filehandle returned by a prior `open` or
    #     `create` call.
    #     '''
    #     # print("Datasync: " + str(fh))

    # @profile
    async def flush(self, fh):
        '''Handle close() syscall.

        *fh* will by an integer filehandle returned by a prior `open` or
        `create` call.

        This method is called whenever a file descriptor is closed. It may be
        called multiple times for the same open file (e.g. if the file handle
        has been duplicated).
        '''
        #TODO: Make threads report back to raise events like no space on disk

        # await trio.to_thread.run_sync(write_new_blocks, self.stat_msg_queue, False)
        
        # write_new_blocks(self.stat_msg_queue, False)
        
        # for idx, w in enumerate(self.writers):
        #     if not w.is_alive():
        #         self.writers.pop(idx)

        # if len(self.writers) <= max_write_workers:
        #     t = threading.Thread(target=write_new_blocks, args=(self.stat_msg_queue, False,))
        #     t.start()
        #     self.writers.append(t)

        
        await self.inodes.commit()
        
        return 0


    async def release(self, fh):
        '''Release open file

        This method will be called when the last file descriptor of *fh* has
        been closed, i.e. when the file is no longer opened by any client
        process.

        *fh* will by an integer filehandle returned by a prior `open` or
        `create` call. Once `release` has been called, no future requests for
        *fh* will be received (until the value is re-used in the return value of
        another `open` or `create` call).

        This method may return an error by raising `FUSEError`, but the error
        will be discarded because there is no corresponding client request.
        '''
                
        self.inode_open_count[fh] -= 1

        if self.inode_open_count[fh] == 0:
            del self.inode_open_count[fh]
            if self.inodes[fh].st_nlink == 0:                
                self._remove(self.inodes[fh].parent_inode, self.inodes[fh].name, await self.getattr(fh))

        await self.inodes.commit()
        

    async def calc_offsets(self, fh):
        blks = [0]                
        [blks.append(x.raw_size+blks[len(blks)-1]) for x in self.inodes[fh].data]
        blks.pop(0)
        i = self.inodes[fh]
        i.offsets = blks
        del self.inodes[fh]
        self.inodes[fh] = i

        return True

    # @profile
    async def write(self, fh, offset, buf):        
        f = self.inodes[fh]
        
        end_offset = offset + len(buf)            
        
        if len(f.data) != 0 and f.size != 0 and end_offset < f.size:
            try:                            
                start_diff, start_block, end_block, data = await self.retrieve_data(fh, offset, len(buf), whole=True)
                buf = bytearray(buf)
                
                data[max(0,start_diff):end_offset-offset+max(0,start_diff)] = buf
                new_data = await dedup(data, self.stat_msg_queue)                

                for i in range(start_block, end_block+1):
                    try:
                        f.data.pop(i)
                    except IndexError:
                        break
                    except KeyError:
                        break
                                
                for d in new_data:                    
                    f.data.insert(start_block, d)
                    start_block = start_block + 1

            except ValueError:
                LogEvent(("ERROR",traceback.format_exc()))                
                return 0        
        else:        
            data = f.data
            data += await dedup(buf, self.stat_msg_queue)
            f.data = data            

        f.size = max(f.size, offset+len(buf))

        f.mtime_ns = time.time_ns()
        self.inodes[fh] = f

        if len(write_buffer) >= write_buffer_size/2 and len(self.writers) < max_write_workers:
            await self.flush(fh)
                
        return len(buf)       

'''

VERATYFS OPERATIONS AND NON OS DEPENDENT CODE
DEDUP, COMPRESSION, WRITE, DELETE, INDEXES MAINTAINANCE

'''

def get_usage(stat_msg_queue):
    """    
    Return drive virtual (undeduped size) and physical (deduped_size) utilization
    :return: Undeduped Data Un-Compressed, Undeduped Data Compressed, Deduped Data Compressed, Deduped Data Removed, Compression rate
    """    
    global hash_table    

    undeduped_compressed = 0
    undeduped_uncompressed = 0
    deduped_compressed = 0
    compression_rate = 0.0

    ht_copy = copy.copy(hash_table.d)
    for k in ht_copy:
        try:
            if not hash_table[k].DELETED:
                undeduped_uncompressed = undeduped_uncompressed + (ht_copy[k].uses * hash_table[k].deflated_size)
                undeduped_compressed = undeduped_compressed + (max(0, ht_copy[k].uses) * hash_table[k].size)
                deduped_compressed = deduped_compressed + hash_table[k].size
        except KeyError:
            continue
    del ht_copy
   
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
    
    return undeduped_uncompressed, undeduped_compressed, deduped_compressed, (undeduped_compressed-deduped_compressed), compression_rate


def garbage_collector(stat_msg_queue):    
    global free_blocks
    global hash_table    
    global fragmentation
    global datastore
    global mirror_datastore
    
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
            if hash_table[remove].uses <= -1:
                hash_table[remove].DELETED = True
                if hash_table[remove].DELETION_TIME is not None and time.time() - hash_table[remove].DELETION_TIME > 5:
                    if hash_table[remove].size <= small_block_limit:
                        r = delete_small_block(remove, mirror_datastore, stat_msg_queue)
                        if not r:
                            LogEvent(("ERROR","DEBUG: Block could not be deleted. File "+ str(remove) +" is now orphan. Please manually delete."))                            
                    elif backend == 'rocksdb':
                        datastore[hash_table[remove].chunk].commit_next_write_position = datastore[hash_table[remove].chunk].commit_next_write_position - hash_table[remove].size
                        rocksdb_delete(remove, datastore[hash_table[remove].chunk])                        
                        fragmentation['free_size'] = 0
                        fragmentation['free_count'] = 0
                    else:
                        try:
                            free_blocks[hash_table[remove].chunk].get(hash_table[remove].size).append(hash_table[remove].offset)                    
                        except AttributeError:
                            free_blocks[hash_table[remove].chunk].insert(hash_table[remove].size, [hash_table[remove].offset])
                        finally:
                            fragmentation['free_size'] = fragmentation['free_size'] + hash_table[remove].size
                            fragmentation['free_count'] = fragmentation['free_count'] + 1            
                elif hash_table[remove].DELETION_TIME is None:
                    hash_table[remove].DELETION_TIME = time.time()                    
            else:
                if random.randrange(1,q_random) == 1:
                    stat_msg_queue.put({'INFO:GCProgress' : str((len(GC.add_uses) + len(GC.remove_uses)))})

    except IndexError:
        pass
    hash_table.commit()
    return True


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
    global free_blocks

    in_index = False

    if idx in hash_table:
        in_index = True    

    if add:
        h = hash_table[idx]
        h.uses = h.uses + 1
        hash_table[idx] = h        

    else:
        try:
            if in_index and hash_table[idx].uses - hash_table[idx].uses <= 0:
                if in_index:
                    if hash_table[idx].chunk in free_blocks and free_blocks[hash_table[idx].chunk].get(hash_table[idx].size) is not None:
                        free_blocks[hash_table[idx].chunk].get(hash_table[idx].size).append(hash_table[idx].offset)                    
                    else:
                        free_blocks[hash_table[idx].chunk].insert(hash_table[idx].size, [hash_table[idx].offset])                    
                        
                    GC.remove_uses.append(idx)
                    h = hash_table[idx]
                    h.DELETED = True
                    h.DELETION_TIME = time.time()
                    hash_table[idx] = h
                    try:
                        if h.size >= small_block_limit:
                            del small_block_read_cache[idx]
                        else:
                            del read_cache[idx]
                    except KeyError:
                        pass                
            elif in_index:                
                h = hash_table[idx]
                h.uses = h.uses + 1
                hash_table[idx] = h                
        except Exception:
            LogEvent(("ERROR",traceback.format_exc()))            
            raise IOError

    hash_table.commit()
    
    return True


# @profile
def write_new_blocks(stat_msg_queue:Queue, daemon:bool):
    '''
    Writes a series of blocks that where quede

    :param _queued_writes: data to be written
    :return: Tuple with the result of the operation and position (block) that the data has been written to
    '''
    global chunk_size
    global datastore
    global mirror_datastore
    global free_blocks    
    global hash_table
    global fragmentation
    
    registers_processed = 0
    bytes_processed = 0
    writing = True    
    start_time = time.time()
    
    while writing:# not _queued_writes.empty(): #writing
        try:            
            q = write_buffer.popleft()
            
            try:
                if backend != 'rocksdb' and len(q['compressed_data']) <= small_block_limit:
                    if q['compressed']:                            
                        q['chunk'] = -1
                        q['block'] = -1
                        written = write_small_block(q['hash'], q['compressed_data'], mirror_datastore, stat_msg_queue)
                    else:                            
                        q['chunk'] = -1
                        q['block'] = -1
                        written = write_small_block(q['hash'], q['data'], mirror_datastore, stat_msg_queue)
                    if random.randrange(0,10) == 0:
                        stat_msg_queue.put_nowait({'INFO:WriteSpeed' : written/(time.time()-start_time)})    
                else:                        
                    try:                            
                        written, q, datastore, mirror_datastore, free_blocks, fragmentation = commit_data(q,datastore, mirror_datastore, free_blocks, fragmentation)
                        
                        if random.randrange(0,10) == 0:
                            stat_msg_queue.put_nowait({'INFO:WriteSpeed' : written/(time.time()-start_time)})
                        
                    except:
                        stat_msg_queue.put_nowait({'DEBUG' : "Error writing data to disk"})                            
                        LogEvent(("ERROR",traceback.format_exc()))
                        raise FUSEError(errno.ENOSPC)
                try:                    
                    del write_read_cache[q['hash']]                    
                except KeyError:
                    print("cache error")
                    pass

                q['result'] = True             
                b = Block()
                b.hash = q['hash']
                b.chunk = q['chunk']
                b.offset = q['block']
                b.size = len(q['compressed_data'])
                b.deflated_size = len(q['data'])
                b.compressed = q['compressed']
                hash_table[q['hash']] = b
                update_index(q['hash'], q['chunk'], True, stat_msg_queue)

                if len(q['compressed_data']) <= small_block_limit:
                    small_block_read_cache[q['hash']] = q['data']         

                registers_processed = registers_processed + 1
                if q['compressed']:
                    bytes_processed = bytes_processed + len(q['compressed_data'])
                else:
                    bytes_processed = bytes_processed + len(q['data'])      
                continue                    

            except KeyError:
                raise FUSEError(errno.ENOSPC)
            except Exception:
                LogEvent(("ERROR",traceback.format_exc()))

        except IndexError:
            if not daemon:  
                writing = False
            else:
                if backend == 'mmap':
                    for ds in datastore:
                        ds.commit_next_write_position()            
                
                time.sleep(2)
                continue
       

    hash_table.commit()


# @profile
async def dedup(data, stat_msg_queue):
    global hash_table    
    global read_cache
    global write_buffer
    global write_read_cache

    blk_list = []    

    start_time = time.time()
    bytes_processed = 0


    if type(data) != memoryview:
        data = bytearray(data) # memoryview(data)
        
        if backend != 'rocksdb' and len(data) <= min_blk_size:
            _hashed_data = hash_data(data)
            blk_list.append(0)
            if _hashed_data in hash_table:
                blk_list[len(blk_list)-1] = FileBlock(_hashed_data, hash_table[_hashed_data].size, hash_table[_hashed_data].deflated_size)
                read_cache[_hashed_data] = data
                update_index(_hashed_data)                
            else:
                q = {'idx':len(blk_list)-1, 'hash':_hashed_data, 'data': bytearray(data), 'result': False}
                q['compressed'], q['compressed_data'] = False, q['data'] #await compress_data(q['data'])
                q['creation_time'] = time.time()                                
                blk_list[q['idx']] = FileBlock(_hashed_data, len(q['compressed_data']), len(q['data']))
                write_read_cache[_hashed_data] = data
                write_buffer.append(q)
                # write_buffer.put(q, block=True)

                '''
                Creates the HashTable entry in advance, so we can identify duplicated data before
                running write_new_data
                '''
                b = Block()
                b.hash = q['hash']                
                b.size = len(q['compressed_data'])
                b.deflated_size = len(q['data'])
                b.compressed = q['compressed']
                hash_table[q['hash']] = b

                bytes_processed = bytes_processed + len(data)
        else:
            ch = await variable_chunks(data)
            for c in ch:
                blk_list.append(0)

                if c.hash in hash_table:                    
                    blk_list[len(blk_list)-1] = FileBlock(c.hash, hash_table[c.hash].size, hash_table[c.hash].deflated_size)
                    read_cache[c.hash] = c.data
                    update_index(c.hash)
                else:

                    q = {'idx':len(blk_list)-1, 'hash':c.hash, 'data': bytearray(c.data), 'result': False}
                    q['compressed'], q['compressed_data'] = await compress_data(q['data'])
                    q['creation_time'] = time.time()                    
                    blk_list[q['idx']] = FileBlock(c.hash, len(q['compressed_data']), len(q['data']))
                    write_read_cache[c.hash] = c.data
                    write_buffer.append(q)
                    # write_buffer.put(q, block=True)

                    '''
                    Creates the HashTable entry in advance, so we can identify duplicated data before
                    running write_new_data
                    '''
                    b = Block()
                    b.hash = q['hash']                
                    b.size = len(q['compressed_data'])
                    b.deflated_size = len(q['data'])
                    b.compressed = q['compressed']
                    hash_table[q['hash']] = b

                bytes_processed = bytes_processed + len(c.data)

    else:        
        LogEvent(("ERROR",traceback.format_exc()))
        raise ValueError
    
    if random.randrange(0,10) == 1:
        stat_msg_queue.put({'INFO:DedupSpeed' : bytes_processed/ (time.time()- start_time)})   
    
    return blk_list

# @profile
def get_file_data(stat_msg_queue, blklst, start_block=None, end_block=None, offset=0, end_offset=0):    
    global datastore
    global allocation_unit
    global hash_table
    global read_cache

    data = bytearray()
        
    for b in blklst[start_block:end_block+1]:

        d = seek_in_cache(b.hash)
        if d is not None:            
            if len(d) != b.raw_size:
                LogEvent(("DEBUG","Invalid cache entry"))
                d = None
                try:
                    del read_cache[b.hash]
                except:
                    pass
        
        if d is None and hash_table[b.hash].chunk == -1:            
            try:
                d = read_small_block(b.hash, stat_msg_queue)
                if hash_table[b.hash].compressed:
                        d = decompress_data(d)   # check if the data has been compressed or not. If it was, decompress it, otherwise return data as read                    
                small_block_read_cache[b.hash] = d                
            except Exception:
                LogEvent(("ERROR",traceback.format_exc()))
                raise IOError

        if d is None:
            try:
                d = read_data(hash_table[b.hash].hash, hash_table[b.hash].offset, datastore[hash_table[b.hash].chunk], b.size, hash_table)
                
            except KeyError:                
                d = seek_in_cache(b.hash)
                if d is not None:
                    break
                else: 
                    d = read_data(hash_table[b.hash].hash, hash_table[b.hash].offset, datastore[hash_table[b.hash].chunk], b.size, hash_table)

        if d is not None and b.hash == hash_data(d):            
            read_cache[b.hash] = d
            # TODO: This is slow. To improve, store parts in a list and use join
            data = data + d

        else:
            if d is None:
                LogEvent(("ERROR",'Block not found!!!'))
            else:                
                LogEvent(("ERROR",'Hash of the data at [def get_file_data], from requested location, does not seem to match the requested hash'))
            raise IOError         
        
    return data


def seek_in_cache(_blk_hash):
    global cache_misses
    global cache_hits

    try:
        r = read_cache[_blk_hash]
        cache_hits = cache_hits + 1
        return r
    except KeyError:
        try:
            r = small_block_read_cache[_blk_hash]
            cache_hits = cache_hits + 1
            return r
        except KeyError:
            try:
                r = write_read_cache[_blk_hash]
                cache_hits = cache_hits + 1
                return r
            except KeyError:
                cache_misses = cache_misses + 1       
                return None


def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]


async def variable_chunks(data):
    return await hashed_chunks(data)

'''
Return how much of the memory allowance in being used in % fom 0 - 100
'''
def percent_used_mem() -> float:
    return (virtual_memory().used / max_memory_allowance)*100

def persist(stat_msg_queue):
   
    last_time = time.time()    
    while RUNNING:
        if (time.time() - last_time > gc_interval):
            garbage_collector(stat_msg_queue)
            if backend != 'rocksdb':
                persist_data(stat_msg_queue)
            last_time = time.time()
        time.sleep(5)

    
def usage(stat_msg_queue):
    mem = virtual_memory()

    time.sleep(1)
    last_time = time.time()    
    while RUNNING:        
        if (time.time() - last_time > usage_interval):              
            get_usage(stat_msg_queue)
            if randint(1, 5) == 1:
                stat_msg_queue.put({'INFO:TOTAL_MEMORY': mem})
                stat_msg_queue.put({'INFO:Memory (active in use)': unix_memory()})
                stat_msg_queue.put({'INFO:Memory (resident)': resident()})
                stat_msg_queue.put({'INFO:Memory (stack size)': stacksize()})
                stat_msg_queue.put({'INFO:CACHE HIT': cache_hits})
                stat_msg_queue.put({'INFO:CACHE MISS': cache_misses})
                
            last_time = time.time()
        time.sleep(3)
        # prof.dump_stats('get_file_data.lprof')
        # print(".")

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
    global RUNNING

    LogEvent(("INFO","Starting main process coodenator"))
    print("Starting main process coodenator...")
    async with trio.open_nursery() as nursery:
        try:
            LogEvent(("INFO","STARTING"))
            print("Starting: Main...")
            nursery.start_soon(pyfuse3.main, 10, 1000)
        
        except KeyboardInterrupt:
            pyfuse3.close(unmount=True)
            sys.exit(-1)
        except:
            LogEvent(("ERROR",traceback.format_exc()))

'''
MAIN PROGRAM
'''

if __name__ == '__main__':

    from functools import partial

    datastore, mirror_datastore, free_blocks, GC = init_persistance()

    stat_msg_queue = mp.Queue()
    
    stat_sender = mp.Process(target=mq_client.send_message, args=(stat_msg_queue, ))
    stat_sender.start()

    options = parse_args()
    init_logging(options.debug)
    operations = Operations(stat_msg_queue)

    # Try cleaning previous mount
    # try:
    #     os.system("fusermount -u "+options.mountpoint)
    # except:
    #     pass

    fuse_options = set(pyfuse3.default_options)
    fuse_options.add('fsname=VeratyFS')
    fuse_options.add('allow_other')    
    fuse_options.discard('default_permissions')    
    if options.debug_fuse:
        fuse_options.add('debug')  
    pyfuse3.init(operations, options.mountpoint, fuse_options)
    
    try:
        par = partial(parent, stat_msg_queue=stat_msg_queue)
        trio.run(par)
    except KeyboardInterrupt:
        pyfuse3.close(unmount=True)
        RUNNING = False
        LogEvent(("ERROR",traceback.format_exc()))
        print("TODO: REDO THAT FOR MP - Persisting remaining data...")
        os.system("clear")
        # prof.print_stats()
        stat_sender.terminate()
        stat_sender.join(1)
        stat_sender.kill()
    finally:        
        RUNNING = False
        pyfuse3.close(unmount=True)        
        print("Flushing data to disk. This can take some time, please wait.\n Forcing termination can cause data loss!!!")
        LogEvent(("INFO","UnMounting and Flushing data to disk."))
        stat_sender.terminate()
        stat_sender.join(10)
        stat_sender.kill()
        