#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''


'''
import time
import subprocess
import os
import sys
import stat
import shutil
import filecmp
import errno
import pytest
from tempfile import NamedTemporaryFile
from .util import fuse_test_marker, wait_for_mount, umount, cleanup

basename = os.path.join(os.path.dirname(__file__), '..')
TEST_FILE = __file__


with open(TEST_FILE, 'rb') as fh:
    TEST_DATA = fh.read()

def name_generator(__ctr=[0]):
    __ctr[0] += 1
    return 'testfile_%d' % __ctr[0]

def test_veratyfs(tmpdir):
    mnt_dir = str(tmpdir)
    cmdline = [sys.executable,
               os.path.join(basename, 'VeratyFS.py'),
               mnt_dir ]

    print(cmdline)
    mount_process = subprocess.Popen(cmdline, stdin=subprocess.DEVNULL, universal_newlines=True)
    try:
        wait_for_mount(mount_process, mnt_dir)
        tst_write(mnt_dir)
        tst_mkdir(mnt_dir)
        tst_symlink(mnt_dir)
        tst_mknod(mnt_dir)
        tst_chown(mnt_dir)
        tst_chmod(mnt_dir) 
        tst_utimens(mnt_dir)
        #tst_link(mnt_dir)
        tst_rename(mnt_dir)
        tst_readdir(mnt_dir)
        tst_statvfs(mnt_dir)
        #tst_truncate_path(mnt_dir)
        #tst_truncate_fd(mnt_dir)
        tst_unlink(mnt_dir)
        print("ALL OK")
    except:
        cleanup(mount_process, mnt_dir)
        raise
    else:
        umount(mount_process, mnt_dir)

def checked_unlink(filename, path, isdir=False):
    print("    checked unlink")
    print("    ================")
    fullname = os.path.join(path, filename)
    if isdir:
        os.rmdir(fullname)
        time.sleep(0.01)
    else:
        os.unlink(fullname)
        time.sleep(0.01)
    with pytest.raises(OSError) as exc_info:
        st = os.stat(fullname)
        print(st)
    assert exc_info.value.errno == errno.ENOENT
    assert filename not in os.listdir(path)

def tst_mkdir(mnt_dir):
    print("Testing mkdir")
    print("================")
    dirname = name_generator()
    fullname = mnt_dir + "/" + dirname
    os.mkdir(fullname)
    fstat = os.stat(fullname)
    assert stat.S_ISDIR(fstat.st_mode)
    assert os.listdir(fullname) ==  []
    assert fstat.st_nlink in (1,2)
    assert dirname in os.listdir(mnt_dir)
    checked_unlink(dirname, mnt_dir, isdir=True)

def tst_symlink(mnt_dir):
    print("Testing symlink")
    print("================")
    linkname = name_generator()
    fullname = mnt_dir + "/" + linkname
    os.symlink("/home/jardel/link_target", fullname)
    fstat = os.lstat(fullname)
    assert stat.S_ISLNK(fstat.st_mode)
    assert os.readlink(fullname) == "/home/jardel/link_target"
    assert fstat.st_nlink == 1
    assert linkname in os.listdir(mnt_dir)
    checked_unlink(linkname, mnt_dir)

def tst_mknod(mnt_dir):
    print("Testing mknod")
    print("================")
    filename = os.path.join(mnt_dir, name_generator())
    shutil.copyfile(TEST_FILE, filename)
    fstat = os.lstat(filename)
    assert stat.S_ISREG(fstat.st_mode)
    assert fstat.st_nlink == 1
    assert os.path.basename(filename) in os.listdir(mnt_dir)
    assert filecmp.cmp(TEST_FILE, filename, False)
    checked_unlink(filename, mnt_dir)

def tst_chown(mnt_dir):
    print("Testing chown")
    print("================")
    filename = os.path.join(mnt_dir, name_generator())
    os.mkdir(filename)
    fstat = os.lstat(filename)
    uid = fstat.st_uid
    gid = fstat.st_gid

    uid_new = uid + 1
    os.chown(filename, uid_new, -1)
    fstat = os.lstat(filename)
    assert fstat.st_uid == uid_new
    assert fstat.st_gid == gid

    gid_new = gid + 1
    os.chown(filename, -1, gid_new)
    fstat = os.lstat(filename)
    assert fstat.st_uid == uid_new
    assert fstat.st_gid == gid_new

    checked_unlink(filename, mnt_dir, isdir=True)

def tst_chmod(mnt_dir):
    print("Testing chmod")
    print("================")
    filename = os.path.join(mnt_dir, name_generator())
    os.mkdir(filename)
    fstat = os.lstat(filename)
    mode = stat.S_IMODE(fstat.st_mode)

    mode_new = 0o640
    assert mode != mode_new
    os.chmod(filename, mode_new)
    fstat = os.lstat(filename)
    assert stat.S_IMODE(fstat.st_mode) == mode_new

    checked_unlink(filename, mnt_dir, isdir=True)

def tst_write(mnt_dir):
    print("Testing write")
    print("================")
    name = os.path.join(mnt_dir, name_generator())
    shutil.copyfile(TEST_FILE, name)
    assert filecmp.cmp(name, TEST_FILE, False)
    checked_unlink(name, mnt_dir)

def tst_unlink(mnt_dir):
    print("Testing unlink")
    print("================")
    name = os.path.join(mnt_dir, name_generator())
    data1 = b'foo'
    data2 = b'bar'

    with open(os.path.join(mnt_dir, name), 'wb+', buffering=0) as fh:
        fh.write(data1)
        checked_unlink(name, mnt_dir)
        fh.write(data2)
        fh.seek(0)
        assert fh.read() == data1+data2

def tst_statvfs(mnt_dir):
    print("Testing statvfs")
    print("================")
    os.statvfs(mnt_dir)

def tst_link(mnt_dir):
    print("Testing link")
    print("================")
    name1 = os.path.join(mnt_dir, name_generator())
    name2 = os.path.join(mnt_dir, name_generator())
    shutil.copyfile(TEST_FILE, name1)
    assert filecmp.cmp(name1, TEST_FILE, False)
    os.link(name1, name2)

    fstat1 = os.lstat(name1)
    fstat2 = os.lstat(name2)

    assert fstat1 == fstat2
    assert fstat1.st_nlink == 2

    assert os.path.basename(name2) in os.listdir(mnt_dir)
    assert filecmp.cmp(name1, name2, False)
    os.unlink(name2)
    fstat1 = os.lstat(name1)
    assert fstat1.st_nlink == 1
    os.unlink(name1)

def tst_rename(mnt_dir):
    print("Testing rename")
    print("================")
    name1 = os.path.join(mnt_dir, name_generator())
    name2 = os.path.join(mnt_dir, name_generator())
    shutil.copyfile(TEST_FILE, name1)

    assert os.path.basename(name1) in os.listdir(mnt_dir)
    assert os.path.basename(name2) not in os.listdir(mnt_dir)
    assert filecmp.cmp(name1, TEST_FILE, False)

    fstat1 = os.lstat(name1)
    os.rename(name1, name2)
    fstat2 = os.lstat(name2)

    assert fstat1 == fstat2
    assert filecmp.cmp(name2, TEST_FILE, False)
    assert os.path.basename(name1) not in os.listdir(mnt_dir)
    assert os.path.basename(name2) in os.listdir(mnt_dir)
    os.unlink(name2)

def tst_readdir(mnt_dir):
    print("Testing readdir")
    print("================")
    dir_ = os.path.join(mnt_dir, name_generator())
    file_ = dir_ + "/" + name_generator()
    subdir = dir_ + "/" + name_generator()
    subfile = subdir + "/" + name_generator()

    os.mkdir(dir_)
    shutil.copyfile(TEST_FILE, file_)
    os.mkdir(subdir)
    shutil.copyfile(TEST_FILE, subfile)

    listdir_is = os.listdir(dir_)
    listdir_is.sort()
    listdir_should = [ os.path.basename(file_), os.path.basename(subdir) ]
    listdir_should.sort()
    assert listdir_is == listdir_should

    os.unlink(file_)
    os.unlink(subfile)
    os.rmdir(subdir)
    os.rmdir(dir_)

def tst_truncate_path(mnt_dir):
    print("Testing truncate_path")
    print("================")
    assert len(TEST_DATA) > 1024

    filename = os.path.join(mnt_dir, name_generator())
    with open(filename, 'wb') as fh:
        fh.write(TEST_DATA)

    fstat = os.stat(filename)
    size = fstat.st_size
    assert size == len(TEST_DATA)

    # Add zeros at the end
    os.truncate(filename, size + 1024)
    assert os.stat(filename).st_size == size + 1024
    with open(filename, 'rb') as fh:
        assert fh.read(size) == TEST_DATA
        assert fh.read(1025) == b'\0' * 1024

    # Truncate data
    os.truncate(filename, size - 1024)
    assert os.stat(filename).st_size == size - 1024
    with open(filename, 'rb') as fh:
        assert fh.read(size) == TEST_DATA[:size-1024]

    os.unlink(filename)

def tst_truncate_fd(mnt_dir):
    print("Testing truncate_fd")
    print("================")
    assert len(TEST_DATA) > 1024
    with NamedTemporaryFile('w+b', 0, dir=mnt_dir) as fh:
        fd = fh.fileno()
        fh.write(TEST_DATA)
        fstat = os.fstat(fd)
        size = fstat.st_size
        assert size == len(TEST_DATA)

        # Add zeros at the end
        os.ftruncate(fd, size + 1024)
        assert os.fstat(fd).st_size == size + 1024
        fh.seek(0)
        assert fh.read(size) == TEST_DATA
        assert fh.read(1025) == b'\0' * 1024

        # Truncate data
        os.ftruncate(fd, size - 1024)
        assert os.fstat(fd).st_size == size - 1024
        fh.seek(0)
        assert fh.read(size) == TEST_DATA[:size-1024]

def tst_utimens(mnt_dir, ns_tol=0):
    print("Testing utimens")
    print("================")
    filename = os.path.join(mnt_dir, name_generator())
    os.mkdir(filename)
    fstat = os.lstat(filename)

    atime = fstat.st_atime + 42.28
    mtime = fstat.st_mtime - 42.23
    atime_ns = fstat.st_atime_ns + int(42.28*1e9)
    mtime_ns = fstat.st_mtime_ns - int(42.23*1e9)
    os.utime(filename, None, ns=(atime_ns, mtime_ns))

    fstat = os.lstat(filename)

    assert abs(fstat.st_atime - atime) < 1e-3
    assert abs(fstat.st_mtime - mtime) < 1e-3
    assert abs(fstat.st_atime_ns - atime_ns) <= ns_tol
    assert abs(fstat.st_mtime_ns - mtime_ns) <= ns_tol

    checked_unlink(filename, mnt_dir, isdir=True)

def tst_passthrough(src_dir, mnt_dir):
    print("Testing passthrough")
    print("================")
    # Test propagation from source to mirror
    name = name_generator()
    src_name = os.path.join(src_dir, name)
    mnt_name = os.path.join(mnt_dir, name)
    assert name not in os.listdir(src_dir)
    assert name not in os.listdir(mnt_dir)
    with open(src_name, 'w') as fh:
        fh.write('Hello, world')
    assert name in os.listdir(src_dir)
    assert name in os.listdir(mnt_dir)
    assert_same_stats(src_name, mnt_name)

    # Test propagation from mirror to source
    name = name_generator()
    src_name = os.path.join(src_dir, name)
    mnt_name = os.path.join(mnt_dir, name)
    assert name not in os.listdir(src_dir)
    assert name not in os.listdir(mnt_dir)
    with open(mnt_name, 'w') as fh:
        fh.write('Hello, world')
    assert name in os.listdir(src_dir)
    assert name in os.listdir(mnt_dir)
    assert_same_stats(src_name, mnt_name)

    # Test propagation inside subdirectory
    name = name_generator()
    src_dir = os.path.join(src_dir, 'subdir')
    mnt_dir = os.path.join(mnt_dir, 'subdir')
    os.mkdir(src_dir)
    src_name = os.path.join(src_dir, name)
    mnt_name = os.path.join(mnt_dir, name)
    assert name not in os.listdir(src_dir)
    assert name not in os.listdir(mnt_dir)
    with open(mnt_name, 'w') as fh:
        fh.write('Hello, world')
    assert name in os.listdir(src_dir)
    assert name in os.listdir(mnt_dir)
    assert_same_stats(src_name, mnt_name)

def assert_same_stats(name1, name2):
    # print("asserting same_stats")
    # print("================")
    stat1 = os.stat(name1)
    stat2 = os.stat(name2)

    for name in ('st_atime_ns', 'st_mtime_ns', 'st_ctime_ns',
                 'st_mode', 'st_ino', 'st_nlink', 'st_uid',
                 'st_gid', 'st_size'):
        assert getattr(stat1, name) == getattr(stat2, name)


if __name__ == '__main__':
    print(sys.argv[0])
    print(sys.argv[1])
    test_veratyfs(sys.argv[1])