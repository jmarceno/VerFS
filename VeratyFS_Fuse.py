#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''


'''

import os
import sys

# If we are running from the pyfuse3 source directory, try
# to load the module from there first.
basedir = os.path.abspath(os.path.join(os.path.dirname(sys.argv[0]), '..'))
if (os.path.exists(os.path.join(basedir, 'setup.py')) and
    os.path.exists(os.path.join(basedir, 'src', 'pyfuse3.pyx'))):
    sys.path.insert(0, os.path.join(basedir, 'src'))

import pyfuse3
import errno
import stat
from time import time
import sqlite3
import logging
from collections import defaultdict
from pyfuse3 import FUSEError
from argparse import ArgumentParser
import trio
import traceback

from datastructures import *
from configurations import inodes, contents, allocation_unit
from fuse_operations import *
from persistence import init_persistance, persist_data
from stats import unix_memory, resident, stacksize

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

    enable_writeback_cache = True
    global contents
    global inodes

    def __init__(self):
        super(Operations, self).__init__()
        self.inodes = inodes
        self.contents = contents
        self.inode_open_count = defaultdict(int)
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
        self.inodes.insert(pyfuse3.ROOT_INODE, new_file)

        root_dir = Directory_Inode([0])
        root_dir.name = b'..'
        root_dir.parent_inode = pyfuse3.ROOT_INODE
        root_dir.inode = pyfuse3.ROOT_INODE
        self.contents.insert(root_dir.name, root_dir)



    async def lookup(self, inode_p, name, ctx=None):
        if name == '.':
            inode = inode_p
        elif name == '..':
            inode = self.contents.get(name).inode
        else:
            try:                
                inode = self.contents.get(name).inode
                # print("Debug: Inode not found. What to do? I don know.")
                # inode = self.get_row("SELECT * FROM contents WHERE name=? AND parent_inode=?",
                #                      (name, inode_p))['inode']
            except TypeError:
                # print(traceback.format_exc())
                raise(pyfuse3.FUSEError(errno.ENOENT))
            except AttributeError:
                raise(pyfuse3.FUSEError(errno.ENOENT))
            
            if inode is None:
                raise(pyfuse3.FUSEError(errno.ENOENT))

        return await self.getattr(inode, ctx)

    
    def count_entries(self, inode):
        dirs = list(self.contents.values())   # TODO: Escaneia todos os diretórios. Péssimo para performance. Encontrar uma forma melhor de encontrar todas as vezes que um inode é referenciado
        entries = 0
        for i in dirs:
            if i.inode == inode:
                entries = entries + 1
        
        return entries


    def count_parent_entries(self, inode):
        dirs = list(self.contents.values())   # TODO: Escaneia todos os diretórios. Péssimo para performance. Encontrar uma forma melhor de encontrar todas as vezes que um inode é referenciado
        entries = 0
        for i in dirs:
            if i.parent_inode == inode:
                entries = entries + 1
        
        return entries


    async def getattr(self, inode, ctx=None):        

        f = self.inodes.get(inode)

        entry = pyfuse3.EntryAttributes()
        entry.st_ino = inode
        entry.generation = 0
        entry.entry_timeout = 300
        entry.attr_timeout = 300
        entry.st_mode = f.mode

        entry.st_nlink = self.count_entries(inode)

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
        return self.inodes.get(inode).target        

    async def opendir(self, inode, ctx):
        return inode

    async def readdir(self, inode, off, token):
        if off == 0:
            off = -1

        dir_lst = []        
        for d in list(self.contents.values()): #TODO: Mesmo problema da funcao anterior
            if d.parent_inode == inode:
                dir_lst.append(d)
        
        for d in dir_lst:
            pyfuse3.readdir_reply(token, d.name, await self.getattr(d.inode), d.row_id)


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
        if self.count_parent_entries(entry.st_ino) > 0:
            raise pyfuse3.FUSEError(errno.ENOTEMPTY)
        
        for e in list(self.contents.keys()): #TODO: LENTO MUDAR
            if self.contents.get(e).name == name and self.contents.get(e).parent_inode == inode_p:
                self.contents.pop(e)       

        if entry.st_nlink == 1 and entry.st_ino not in self.inode_open_count:
            self.inodes.pop(entry.st_ino)

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
            old = self.contents.get(inode_p_old)
            old.name = name_new
            old.parent_inode = inode_p_new
            self.inodes.update(inode_p_old, old)


    def _replace(self, inode_p_old, name_old, inode_p_new, name_new,
                 entry_old, entry_new):

        if self.count_parent_entries(entry_new.st_ino) > 0:        
            raise pyfuse3.FUSEError(errno.ENOTEMPTY)

        old = self.contents.pop(name_old)
        new_d = Directory_Inode(self.contents)
        new_d.name = old.name
        new_d.inode = entry_old.st_ino
        new_d.parent_inode = old.parent_inode
        self.contents.insert(new_d.inode, new_d)

        if entry_new.st_nlink == 1 and entry_new.st_ino not in self.inode_open_count:
            for i in list(self.inodes.keys()):
                if self.inodes.get(i).id == entry_new.st_ino:
                    self.inodes.pop(i)             


    async def link(self, inode, new_inode_p, new_name, ctx):
        entry_p = await self.getattr(new_inode_p)
        if entry_p.st_nlink == 0:
            log.warning('Attempted to create entry '+ str(new_name) + 'with unlinked parent '+ str(new_inode_p))
            raise FUSEError(errno.EINVAL)

        d = Directory_Inode(self.contents)
        d.name = new_name
        d.inode = inode
        d.parent_inode = new_inode_p
        self.contents.insert(inode, d)

        return await self.getattr(inode)

    async def setattr(self, inode, attr, fields, fh, ctx):

        old_inode = self.inodes.get(inode)

        if fields.update_size:
            size = 0
            f = self.inodes.get(inode)
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
            
        self.inodes.get(inode).value = old_inode
        # self.inodes.update(inode,old_inode)

        return await self.getattr(inode)

    async def mknod(self, inode_p, name, mode, rdev, ctx):
        return await self._create(inode_p, name, mode, ctx, rdev=rdev)

    async def mkdir(self, inode_p, name, mode, ctx):
        return await self._create(inode_p, name, mode, ctx)

    async def statfs(self, ctx):
        stat_ = pyfuse3.StatvfsData()

        stat_.f_bsize = allocation_unit
        stat_.f_frsize = allocation_unit

        size = 0 #self.get_row('SELECT SUM(size) FROM inodes')[0]
        for k in list(self.inodes.keys()):
            size = size + self.inodes.get(k).size
        stat_.f_blocks = partition_size_gb // stat_.f_frsize    
        # stat_.f_blocks = size // stat_.f_frsize
        stat_.f_bfree = stat_.f_blocks - (size // allocation_unit)
        # stat_.f_bfree = max(size // stat_.f_frsize, 1024) #TODO: WHAT IS THIS SHIT?
        stat_.f_bavail = stat_.f_bfree

        fs_inodes = len(self.inodes) #self.get_row('SELECT COUNT(id) FROM inodes')[0]
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

        inode = self.inodes.maxKey()+1
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
        self.inodes.insert(new_file.id, new_file)
        
        d = Directory_Inode(self.contents)
        d.name = name
        d.inode = inode
        d.parent_inode = inode_p

        self.contents.insert(name, d)
        
        return await self.getattr(inode)

    async def read(self, fh, offset, length):
        # data = self.get_row('SELECT data FROM inodes WHERE id=?', (fh,))[0]
        # debugpy.debug_this_thread()
        f = None
        for i in list(self.inodes.keys()):
            if self.inodes.get(i).id == fh:
                f = self.inodes.get(i)

        if len(i.data) == 0:
            data = b''
        
        else:        
            if offset >= i.size:
                print("End of File")
                raise IOError
            end_offset = min(self.file_size, offset + length)

            start_blk = 0
            start_diff = 0
            end_blk = 0
            end_diff = 0

            internal_offset = 0
            for blk_number, blk in enumerate(i.data):
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
            for blk_number_e, blk_e in enumerate(i.data):
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
                return get_file_data(i.data, start_blk, end_blk + 1)[offset:end_offset]
            else:
                data = get_file_data(i.data, start_blk, end_blk)

                if i.size == end_offset:
                    data = data[-(end_offset-offset):]
                else:
                    if start_diff < 0:
                        print(start_diff)
                    if start_blk == 0:
                        data = data[offset:]
                    elif offset > 0 and start_diff > 0:
                        data = data[i.data[start_blk].size-start_diff:]
                    if end_diff > 0 and len(data) > (end_offset-offset):
                        data = data[:-end_diff]

            if data == b'':
                print("Fuck it")

            if len(data) != (end_offset - offset):
                print("data problem")
            # if offset > 546870912:
            #     print(offset)
            return data

        if data is None:
            data = b''

        return data

    async def write(self, fh, offset, buf):
        # debugpy.debug_this_thread()
        f = None
        for i in list(self.inodes.keys()):
            if self.inodes.get(i).id == fh:
                f = self.inodes.get(i)

        end_offset = offset + len(buf)
        if end_offset > f.size:
            f.size = end_offset #TODO: SETAR TAMANHO DO ARQUIVO DE FORMA CORRETA

        f.data += dedup(bytes(buf))
        
        self.inodes.get(fh).data = f.data
        self.inodes.get(fh).size = f.size
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


class NoUniqueValueError(Exception):
    def __str__(self):
        return 'Query generated more than 1 result row'


class NoSuchRowError(Exception):
    def __str__(self):
        return 'Query produced 0 result rows'

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
        # -- we exit the nursery block here --
    print("parent: all done!")


async def persist():
    last_time = time.time()
    while True:
        if time.time() - last_time > gc_interval:
            print("DEBUG: Staring Write")
            write_new_blocks(write_buffer)
            garbage_collector()
            persist_data([inodes, contents])
            last_time = time.time()
        await trio.sleep(5)


async def usage():
    last_time = time.time()
    while True:
        if time.time() - last_time > gc_interval:
            print("DEBUG: Usage")
            get_usage()
            print("Memory Used: +"+ humanbytes(unix_memory()))
            print("Resident Memory: "+humanbytes(resident()))
            print("Memory Stack Size: "+humanbytes(stacksize()))
            last_time = time.time()
        await trio.sleep(5)
    

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

    init_persistance()

    
    try:
        trio.run(parent)
    except:
        pyfuse3.close(unmount=False)        
        raise

    pyfuse3.close()
    
