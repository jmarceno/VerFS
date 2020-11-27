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
# TEST_FILE = __file__
# TEST_FILE = os.path.join(os.getcwd(), "tests","test_file.csv")
# TEST_FILE = os.path.join(os.getcwd(), "tests","test_file_medium.zip")
TEST_FILE = os.path.join(os.getcwd(), "tests","test_file_big.zip")

'''
Test it the FS can continue to write after the fragmentation level
has increased as consequence from various files being added and then removed

 The fs has to continue to write "forever" if capacity is available
 (If if can do that, so the defragmention system is woring as intended)

 How to test:
 1. Generate random file
 1.1 Hash this file and store the hash
 2. Copy thi file to the FS
 3. Hash the resulting file
 4. Compare both files TODO: Is the hash needed or Python's FileComp is enough
 5. Delete the file
 6. Repeat 1 to 5 until 10x (or more...configurable) the FS capacity has been written
'''


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
        
    except:
        cleanup(mount_process, mnt_dir)
        raise
    else:
        umount(mount_process, mnt_dir)

def checked_unlink(filename, path, isdir=False):        
    fullname = os.path.join(path, filename)
    if isdir:
        os.rmdir(fullname)        
    else:
        os.unlink(fullname)        
    with pytest.raises(OSError) as exc_info:
        st = os.stat(fullname)        
    assert exc_info.value.errno == errno.ENOENT
    assert filename not in os.listdir(path)

def tst_write(mnt_dir):
    name = os.path.join(mnt_dir, name_generator())
    shutil.copyfile(TEST_FILE, name)       
    filecmp.clear_cache()
    assert filecmp.cmp(name, TEST_FILE, False)
    checked_unlink(name, mnt_dir)

if __name__ == '__main__':
    print(sys.argv[0])
    print(sys.argv[1])
    test_veratyfs(sys.argv[1])