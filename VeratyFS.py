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
from utils import take_closest, offsets
from numba import jit

# import builtins
# import line_profiler
# prof = line_profiler.LineProfiler()
# builtins.__dict__['profile'] = prof


# datastore, free_blocks, key_index, hash_table, fs_meta, GC = init_persistance()

write_buffer_lock = False

wq = mp.Queue() # Fila de mensagens a serem escritas no disco
resq = mp.Queue() # Fila com as respostas das mensagens escritas
swq = mp.Queue() # Fila de escrita de blocos pequenos, estes nao tem fila de retorno


# try:
#     import faulthandler
# except ImportError:
#     pass
# else:
#     faulthandler.enable()

# log = logging.getLogger()

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
    global inodes

    enable_writeback_cache = False
    
    def __init__(self, stat_msg_queue):
        super(Operations, self).__init__()
        
        
        self.inode_open_count = defaultdict(int)
        self.stat_msg_queue = stat_msg_queue

        self.buffers = {}        
        
        try:
            if len(fs_meta[0]) > 0:
                self.inodes = fs_meta[0]                
                self.init_file_system()                
            else:
                self.inodes = fs_meta[0]                
        except TypeError:
            print(traceback.format_exc())
            self.inodes = inodes            
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
                    if self.inodes[x].parent_inode==inode_p and self.inodes[x].name==name:
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

            entry.st_nlink = f.st_nlink #await self.count_entries(inode) # #TODO O sistema vai suportar HARD-LINKS? Caso negativo, apenas retornar 2, funciona

            entry.st_uid = f.uid
            entry.st_gid = f.gid
            entry.st_rdev = f.rdev
            entry.st_size = f.size

            entry.st_blksize = allocation_unit
            entry.st_blocks = 1
            entry.st_atime_ns = f.atime_ns
            entry.st_mtime_ns = f.mtime_ns
            entry.st_ctime_ns = f.ctime_ns

            # del f
        except KeyError:
            raise (pyfuse3.FUSEError(errno.ENOENT))

        return entry


    async def readlink(self, inode, ctx):
        return self.inodes[inode].target        

    async def opendir(self, inode, ctx):
        return inode

    #@profile
    async def readdir(self, inode, off, token):
        dir_entries = [] #self.dirs[inode][off:]
        [dir_entries.append(self.inodes[x]) for y,x in enumerate(self.inodes, off) if self.inodes[x].parent_inode==inode and self.inodes[x].list_on_dir_lookup]        
        #[dir_entries.append(x[1]) for y,x in enumerate(self.dirs[inode], off)]
        try:
            pyfuse3.readdir_reply(token, dir_entries[off].name, await self.getattr(dir_entries[off].inode), off+1)
            # pyfuse3.readdir_reply(token, self.inodes[dir_entries[0]].name, await self.getattr(self.inodes[dir_entries[0]].inode), off+1)
        except IndexError:
            return False

    async def unlink(self, inode_p, name,ctx):
        entry = await self.lookup(inode_p, name)

        if stat.S_ISDIR(entry.st_mode):
            raise pyfuse3.FUSEError(errno.EISDIR)

        # self.inodes[entry.st_ino].parent_inode = -1
        self.inodes[entry.st_ino].lookup_count = self.inodes[entry.st_ino].lookup_count - 1
        self.inodes[entry.st_ino].list_on_dir_lookup = False
        
        # self.inode_open_count[inode] += 1
        # self._remove(inode_p, name, entry)

    async def forget(self, inode_list):
        '''Decrease lookup counts for inodes in *inode_list*
        *inode_list* is a list of ``(inode, nlookup)`` '''

        for n in inode_list:
            try:
                if self.inodes[n[0]].inode != pyfuse3.ROOT_INODE:      
                    self.inodes[n[0]].lookup_count = self.inodes[n[0]].lookup_count - n[1]
                    if self.inodes[n[0]].lookup_count <= 0:
                        self._remove(self.inodes[n[0]].parent_inode, self.inodes[n[0]].name, await self.getattr(n[0]))
                else:
                    self.inodes[n[0]].lookup_count = 1
            except:
                print('Inode does not exist')
                pass


        print("Hey, let's forget it, right?")
        print(inode_list)        

    async def rmdir(self, inode_p, name, ctx):
        entry = await self.lookup(inode_p, name)

        if not stat.S_ISDIR(entry.st_mode):
            raise pyfuse3.FUSEError(errno.ENOTDIR)

        for i in self.inodes: # TODO: Don't know about that...really needed? If so, is really this way?
            if self.inodes[i].parent_inode == entry.st_ino and self.inodes[i].list_on_dir_lookup == True:
                raise FUSEError(errno.ENOTEMPTY)

        # self.inodes[entry.st_ino].parent_inode = -1
        self.inodes[entry.st_ino].lookup_count = self.inodes[entry.st_ino].lookup_count - 1
        # self._remove(inode_p, name, entry, d=True)

    def _remove(self, inode_p, name, entry, d=False):
        # if d:
        #     if len(self.dirs[inode_p]) > 0:
        #         raise pyfuse3.FUSEError(errno.ENOTEMPTY)
        if self.inodes[inode_p].inode != inode_p and self.inodes[inode_p].parent_inode == inode_p and self.inodes[inode_p].name != name:
            raise pyfuse3.FUSEError(errno.ENOTEMPTY)
        
        # if not d:
        #==============
        for k, v in self.inodes.items(): #TODO: BISECT ?
            if v.name == name and v.parent_inode == inode_p:
                try:
                    f = self.inodes.pop(k)                    
                    for i in f.data:
                        update_index(i.hash, add=False, stat_msg_queue=self.stat_msg_queue)                         
                    break
                except KeyError:
                    print(traceback.format_exc())
                    print("Key already removed? Index Inconsistance at self.inodes")

        #================
        # else:
        #     i_copy = copy.copy(self.inodes)
        #     for k, v in i_copy.items(): #TODO: BISECT ?
        #         if (v.name == name and v.parent_inode == inode_p) or v.parent_inode == entry.st_ino:
        #             try:
        #                 f = self.inodes.pop(k)        
        #                 for i in f.data:
        #                     update_index(i.hash, add=False, stat_msg_queue=self.stat_msg_queue)                                                
        #             except KeyError:
        #                 print(traceback.format_exc())
        #                 print("Key already removed? Index Inconsistance at self.inodes")
        
    async def symlink(self, inode_p, name, target, ctx):
        mode = (stat.S_IFLNK | stat.S_IRUSR | stat.S_IWUSR | stat.S_IXUSR |
                stat.S_IRGRP | stat.S_IWGRP | stat.S_IXGRP |
                stat.S_IROTH | stat.S_IWOTH | stat.S_IXOTH)
        return await self._create(inode_p, name, mode, ctx, target=target)


    async def rename(self, inode_p_old, name_old, inode_p_new, name_new, flags, ctx):
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
            self._replace(inode_p_old, name_old, inode_p_new, name_new, entry_old, entry_new)            
        else:
            old = self.inodes[entry_old.st_ino]
            # old = self.inodes[inode_p_old]
            old.name = name_new
            old.parent_inode = inode_p_new
            self.inodes[entry_old.st_ino] = old
            
            

    def _replace(self, inode_p_old, name_old, inode_p_new, name_new, entry_old, entry_new):
        # if stat.S_ISDIR(entry_old.st_mode):
        #     raise pyfuse3.FUSEError(errno.ENOTEMPTY)

        # for c in self.inodes:            
        #     if self.inodes[c].inode != inode_p_old and self.inodes[c].parent_inode == inode_p_old and self.inodes[c].name != name_old: # and str('.trashinfo.') not in str(name_new) and str('.trashinfo.') not in str(name_old):
        #         raise pyfuse3.FUSEError(errno.ENOTEMPTY)
        
        '''Let the inode associated with *name_old* in *parent_inode_old* be
        *inode_moved*, and the inode associated with *name_new* in
        *parent_inode_new* (if it exists) be called *inode_deref*.

        If *inode_deref* exists and has a non-zero lookup count, or if there are
        other directory entries referring to *inode_deref*), the file system
        must update only the directory entry for *name_new* to point to
        *inode_moved* instead of *inode_deref*.'''


        # old = self.inodes.pop(entry_old.st_ino)
        inode_moved = self.inodes[entry_old.st_ino]
        try:
            inode_deref = self.inodes[entry_new.st_ino]
            if inode_deref.lookup_count > 0:
                inode_moved.name = name_new
                inode_moved.parent_inode = inode_p_new                
                self.inodes[entry_old.st_ino] = inode_moved #<- Point the new back to the old?
                inode_deref.list_on_dir_lookup = False                
                # self.inodes[entry_new.st_ino].list_on_dir_lookup = False                
                # self.inodes[entry_new.st_ino].parent_inode = -1
                self.inodes[entry_new.st_ino] = inode_deref
                print('pointed')
                return True            
        except:
            pass
        
        inode_moved.name = name_new
        inode_moved.parent_inode = inode_p_new
        # inode_moved.list_on_dir_lookup = True
        # old.inode = self.gen_inode_number() #max(self.inodes) + 1 #len(self.inodes)+1
        self.inodes[inode_moved.inode] = inode_moved
        print('changed')

        return True
        

    def gen_inode_number(self):        
        return max(self.inodes) + 1


    async def link(self, inode, new_inode_p, new_name, ctx):
        entry_p = await self.getattr(new_inode_p)
        if entry_p.st_nlink == 0:
            log.warning('Attempted to create entry '+ str(new_name) + 'with unlinked parent '+ str(new_inode_p))
            raise FUSEError(errno.EINVAL)
                
        ni = File_Inode(self.gen_inode_number())
        ni.name = new_name
        ni.parent_inode = new_inode_p
        ni.target = inode
        self.inodes[ni.inode] = ni
        self.inodes[inodes].lookup_count = self.inodes[inodes].lookup_count + 1

        return await self.getattr(inode)

    async def setattr(self, inode, attr, fields, fh, ctx):

        old_inode = self.inodes[inode]

        if fields.update_size:
            size = 0            
            for d in old_inode.data:
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
        return pyfuse3.FileInfo(fh=inode, direct_io=False)

    async def access(self, inode, mode, ctx):
        # Yeah, could be a function and has unused arguments
        #pylint: disable=R0201,W0613
        return True

    async def create(self, inode_parent, name, mode, flags, ctx):
        #pylint: disable=W0612
        entry = await self._create(inode_parent, name, mode, ctx)
        self.inode_open_count[entry.st_ino] += 1
        return (pyfuse3.FileInfo(fh=entry.st_ino, direct_io=False), entry)

    async def _create(self, inode_p, name, mode, ctx, rdev=0, target=None):
        if (await self.getattr(inode_p)).st_nlink == 0:
            log.warning('Attempted to create entry '+ str(name) + 'with unlinked parent '+ str(inode_p))
            raise FUSEError(errno.EINVAL)

        #inode =  # max(self.inodes) + 1 # len(self.inodes)+1
        now_ns = time.time_ns()        
        new_file = File_Inode(self.gen_inode_number())
        new_file.mode = mode
        new_file.uid = ctx.uid
        new_file.gid = ctx.gid
        new_file.mtime_ns = now_ns
        new_file.atime_ns = now_ns
        new_file.ctime_ns = now_ns
        new_file.target = target
        new_file.rdev = rdev
        new_file.name = name
        new_file.parent_inode = inode_p
        self.inodes[new_file.inode] = new_file
                               
        return await self.getattr(new_file.inode)

    
    async def read(self, fh, offset, length, whole=False): 
        start_time = time.time()       
        
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
            
            if len(self.inodes[fh].offsets) == 0 or whole:
                blks = [0]                
                [blks.append(x.size+blks[len(blks)-1]) for x in self.inodes[fh].data]
                blks.pop(0)
                self.inodes[fh].offsets  = blks
            
            blk, blk_number = take_closest(self.inodes[fh].offsets, offset)            
            if offset != 0:
                if blk == offset:
                    start_blk = blk_number + 1
                else:
                    start_blk = blk_number
                    start_diff = blk - offset
            
            blk_e, blk_number_e = take_closest(self.inodes[fh].offsets, end_offset)                        
            if blk_e == end_offset:
                end_blk = blk_number_e
            else:
                end_diff = blk_e - end_offset
                end_blk = blk_number_e

            if start_blk == 0 and end_blk == 0:
                if not whole:                    
                    data = get_file_data(self.stat_msg_queue, self.inodes[fh].data, start_blk, end_blk + 1)[offset:end_offset]
                else:
                    # if start_diff < 0:
                    #     start_blk = max(0, start_blk -1)
                    data = get_file_data(self.stat_msg_queue, self.inodes[fh].data, start_blk, end_blk + 1)[offset:end_offset]
                    
                    return start_diff, start_blk, end_blk, data
            else:
                data = get_file_data(self.stat_msg_queue, self.inodes[fh].data, start_blk, end_blk)

                if whole:
                    if start_diff < 0:
                        start_blk = max(0, start_blk -1)
                    return start_diff, start_blk, end_blk, data

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
            

            if len(data) != (end_offset - offset):
                print("Returning wrong length data @ [async def read]. Is this intended?")
                return data

        if data is None:
            data = b''

        # print(humanbytes(len(data)/ (time.time()- start_time))+"/s")
        # print("Time taken -> " + str(time.time()-start) + " Data Length:" + str(len(data)))
        self.stat_msg_queue.put({'INFO:ReadSpeed' : str(len(data)/ (time.time()- start_time))})

        return data
    
    async def fsync(self, fh, datasync):
        '''Flush buffers for open file *fh*

        If *datasync* is true, only the file contents should be
        flushed (in contrast to the metadata about the file).

        *fh* will by an integer filehandle returned by a prior `open` or
        `create` call.
        '''
        # print("Datasync: " + str(fh))

    async def flush(self, fh):
        '''Handle close() syscall.

        *fh* will by an integer filehandle returned by a prior `open` or
        `create` call.

        This method is called whenever a file descriptor is closed. It may be
        called multiple times for the same open file (e.g. if the file handle
        has been duplicated).
        '''
        # print("Flush:" + str(fh))

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
        # print("Release:" + str(fh))

        self.inode_open_count[fh] -= 1

        if self.inode_open_count[fh] == 0:
            del self.inode_open_count[fh]
            # if (await self.getattr(fh)).st_nlink == 0:
            #     self.inodes.pop(fh)
    

    async def write(self, fh, offset, buf):                
        buf = memoryview(buf)
        # f = None

        f = self.inodes[fh]

        # for i in list(self.inodes.keys()):
        #     if self.inodes.get(i).inode == fh:
        #         f = self.inodes[i]
        #         break
        append = False
        if offset >= f.size:
            append = True
        end_offset = offset + len(buf)
        if end_offset > f.size: # and offset==f.size:
            f.size = end_offset

        if offset == 0 and len(self.inodes[fh].offsets) > 0 or len(self.inodes[fh].data) == 0 or end_offset == self.inodes[fh].size or append == True:
            data = f.data
            data += await dedup(buf, self.stat_msg_queue)
            self.inodes[fh].data = data
        
        else: # offset != 0:
            start_diff, start_block, end_block, data = await self.read(fh, offset, len(buf), whole=True)            

            if data == b'':
                data = f.data
                data = await dedup(buf, self.stat_msg_queue)
                self.inodes[fh].data = data

            else:
                buf = bytearray(buf)
                if start_diff < 0:
                    start_diff = start_diff * -1
                for x in range((offset-offset)+start_diff, end_offset-offset):                    
                    try:
                        data[x] = buf.pop(0)
                    except IndexError:
                        data += buf
                        break

                for i in range(start_block, end_block+1):
                    try:
                        self.inodes[fh].data.pop(i)
                    except IndexError:
                        break
                    except KeyError:
                        break
                
                new_data = await dedup(data, self.stat_msg_queue)
                for i in new_data:                
                    self.inodes[fh].data.insert(start_block, i)
                    start_block = start_block + 1
        
        
        self.inodes[fh].size = f.size                
        self.inodes[fh].offsets = []

        # dirs = self.dirs
        # inodes = self.inodes

        
        # inodes = self.inodes            # Coloca os dados do FS de volta na memória compartilhada para pode ser acessada e persistida

        # snapshot = tracemalloc.take_snapshot()
        # top_stats = snapshot.statistics('lineno', cumulative=True)
        # os.system('clear')
        # print("[ Top 10 ]")
        # for stat in top_stats[:20]:
        #     print(stat)

        return len(buf)


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

    key_index_copy = copy.copy(key_index)
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
            if hash_table[remove].uses <= block_negative_limit:
                hash_table[remove].DELETED = True
                if hash_table[remove].DELETION_TIME is not None and time.time() - hash_table[remove].DELETION_TIME > deletion_grace_period:
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
                elif hash_table[remove].DELETION_TIME is None:
                    hash_table[remove].DELETION_TIME = time.time()                    
            else:
                if random.randrange(1,q_random) == 1:
                    stat_msg_queue.put({'INFO:GCProgress' : str((len(GC.add_uses) + len(GC.remove_uses)))})

    except IndexError:
        pass
    
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


async def write_new_blocks(_queued_writes, wq, resq, swq, stat_msg_queue):
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
            if not q['result'] and q['hash'] not in hash_table:
                try:
                    if len(q['compressed_data']) <= small_block_limit:
                        if q['compressed']:                            
                            swq.put((q['hash'], q['compressed_data']))
                            q['chunk'] = -1
                            q['block'] = -1
                        else:                            
                            swq.put((q['hash'], q['data']))                            
                            q['chunk'] = -1
                            q['block'] = -1                        
                    else:
                        for idx, ds in enumerate(datastore):
                            if not ds.IS_FULL:
                                q['block'] = ds.next_write_position
                                if q['compressed']:
                                    ds.next_write_position = ds.next_write_position + len(q['compressed_data'])
                                else:
                                    ds.next_write_position = ds.next_write_position + len(q['data'])
                                q['chunk'] = ds.chunk
                                if ds.next_write_position + max_blk_size > ds.size:
                                    ds.IS_FULL = True
                                break
                            elif idx != len(datastore)-1:
                                continue
                            else:
                                for ds, fb in enumerate(free_blocks):
                                    try:                                    
                                        s = fb.minKey(len(q.compressed_data))
                                        q['block'] = fb.get(s).pop(0)
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
                       
                    q['result'] = True                    
                    hash_table[q['hash']] = Block()
                    hash_table[q['hash']].hash = q['hash']
                    hash_table[q['hash']].chunk = q['chunk']
                    hash_table[q['hash']].offset = q['block']
                    hash_table[q['hash']].size = len(q['compressed_data'])
                    hash_table[q['hash']].deflated_size = len(q['data'])
                    hash_table[q['hash']].compressed = q['compressed']
                    update_index(q['hash'], q['chunk'], True, stat_msg_queue)

                    if len(q['compressed_data']) <= small_block_limit:
                        small_block_read_cache[q['hash']] = q['data']

                    try:
                        del write_read_cache[q['hash']]
                    except KeyError:
                        continue

                    registers_processed = registers_processed + 1
                    if q['compressed']:
                        bytes_processed = bytes_processed + len(q['compressed_data'])
                    else:
                        bytes_processed = bytes_processed + len(q['data'])      
                    continue                    

                except Exception:
                    print(traceback.format_exc()) 
            else:                
                registers_processed = registers_processed + 1                
                if q['compressed']:
                    bytes_processed = bytes_processed + len(q['compressed_data'])
                else:
                    bytes_processed = bytes_processed + len(q['data'])
            del q

        except IndexError:
            writing = False
            write_buffer_lock = False
    
    write_buffer_lock = False


async def dedup(data, stat_msg_queue):    
    # global datastore
    # global free_blocks    
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
                q = {'idx':len(blk_list)-1, 'hash':_hashed_data, 'data': bytearray(data), 'result': False}
                q['compressed'], q['compressed_data'] = await compress_data(q['data'])
                q['creation_time'] = time.time()

                # q = QueuedWrite(len(blk_list)-1, _hashed_data, bytearray(data))
                blk_list[q['idx']] = FileBlock(_hashed_data, len(data))
                write_read_cache[_hashed_data] = data
                write_buffer.append(q)
                bytes_processed = bytes_processed + len(data)
        else:
            ch = await variable_chunks(memoryview(data))
            for c in ch:
                blk_list.append(0)

                if c.hash in hash_table:
                    blk_list[len(blk_list)-1] = FileBlock(c.hash, len(c.data))
                    update_index(c.hash)
                else:
                    q = {'idx':len(blk_list)-1, 'hash':c.hash, 'data': bytearray(c.data), 'result': False}
                    q['compressed'], q['compressed_data'] = await compress_data(q['data'])
                    q['creation_time'] = time.time()
                    # q = QueuedWrite(len(blk_list)-1, c.hash, bytearray(c.data))
                    blk_list[q['idx']] = FileBlock(c.hash, len(c.data))
                    write_read_cache[c.hash] = c.data
                    write_buffer.append(q)

                bytes_processed = bytes_processed + len(c.data)

    else:
        print("Value Error when preparing writes")
        print(traceback.format_exc())
        raise ValueError
    
    if random.randrange(0,10) == 1:
        stat_msg_queue.put({'INFO:DedupSpeed' : bytes_processed/ (time.time()- start_time)})

    await write_new_blocks(write_buffer, wq, resq, swq, stat_msg_queue)
    
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
    
    for b in blklst[start_block:end_block+1]:

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
                raise IOError
    
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
                    mm = mmap.mmap(f.fileno(), length=chunk_size, access=mmap.ACCESS_READ)
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
        
    try:        
        del cached
        del r    
        del start_time
        del bytes_processed
        del d
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


async def variable_chunks(data):
    return await hashed_chunks(data)

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
            
        except KeyboardInterrupt:
            sys.exit(-1)
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
                
                with open(datastore[q['chunk']].path, "r+b") as f:
                    mm = mmap.mmap(f.fileno(), length=datastore[q['chunk']].size, access=mmap.ACCESS_WRITE)
                    # for q, datastore in work:                    
                    mm.seek(q['block'])
                    if q['compressed']:
                        written = mm.write(q['compressed_data'])
                    else:                            
                        written = mm.write(q['data'])           
                    # mm.madvise(mmap.MADV_DONTNEED)                    
                    mm.close()
                    del mm                    
                                    
            except ValueError:
                print("ValueError Writing data to the disk: Chunk:{}, Block:{}, Data Size:{}".format(q['chunk'], q['block'], len(q['compressed_data'])))
                print(traceback.format_exc())
            except queue.Empty:
                continue
            if random.randrange(0,10) == 0:
                stat_msg_queue.put_nowait({'INFO:WriteSpeed' : written/(time.time()-start)})
                        
        else:
            del written
            del bytes_written
            continue
    

async def persist(stat_msg_queue):

    # ctx = mp.get_context('fork')
        
    # wq = mp.Queue() # Fila de mensagens a serem escritas no disco
    # resq = mp.Queue() # Fila com as respostas das mensagens escritas
    # swq = mp.Queue() # Fila de escrita de blocos pequenos, estes nao tem fila de retorno
    
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
   
    await trio.sleep(5)
    last_time = time.time()    
    while True:
        if (time.time() - last_time > gc_interval or len(write_buffer) > write_buffer_size) and not write_buffer_lock:    
            # if len(write_buffer) > 0:                
            #     # await trio.to_thread.run_sync(write_new_blocks, write_buffer, wq, resq, swq, stat_msg_queue)
            #     await write_new_blocks(write_buffer, wq, resq, swq, stat_msg_queue)
            # await garbage_collector(stat_msg_queue)
            # await persist_data([inodes, dirs], stat_msg_queue)
            await trio.to_thread.run_sync(garbage_collector, stat_msg_queue)
            await trio.to_thread.run_sync(persist_data, [inodes, 0], stat_msg_queue)
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
        await trio.sleep(60)


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

    await trio.sleep(1)
    last_time = time.time()    
    while True:        
        if (time.time() - last_time > usage_interval):  
            await trio.to_thread.run_sync(get_usage, stat_msg_queue)
            stat_msg_queue.put({'INFO:TOTAL_MEMORY': mem})
            stat_msg_queue.put({'INFO:Memory (active in use)': unix_memory()})
            stat_msg_queue.put({'INFO:Memory (resident)': resident()})
            stat_msg_queue.put({'INFO:Memory (stack size)': stacksize()})
            last_time = time.time()
        await trio.sleep(60)
        # prof.dump_stats('readdir_profile.lprof')        

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
    except KeyboardInterrupt:
        pyfuse3.close(unmount=True)
        print(traceback.format_exc())
        print("TODO: REDO THAT FOR MP - Persisting remaining data...")
        os.system("clear")
        # prof.print_stats()
        stat_sender.join(2)
        stat_sender.kill()
    finally:
        pyfuse3.close()