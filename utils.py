from numba import jit
from numba.typed import List


from bisect import bisect_left, bisect_right

import rocksdb

def take_closest(myList, myNumber, left=True):
    """
    Assumes myList is sorted. Returns closest value to myNumber.

    If two numbers are equally close, return the smallest number.
    """
    if left:
        pos = bisect_left(myList, myNumber)
    else:
        pos = bisect_right(myList, myNumber)

    if pos == 0:
        return myList[0], pos
    if pos == len(myList):
        return myList[-1], pos-1
    # before = myList[pos - 1]
    return myList[pos], pos
    # after = myList[pos]
    # if after - myNumber < myNumber - before:
    #    return after, pos
    # else:
    #    return before, pos-1


@jit(nopython=True) # Set "nopython" mode for best performance, equivalent to @njit
def offsets(data):

    offs = List()
    offs.append(0)
    [offs.append(x[1]+offs[len(offs)-1]) for x in data]
    offs.pop(0)
    
    # offsets = List()
    # [offsets.append(x+offsets[len(offsets)-1]) for x in data]
    # offsets.pop(0)

    return offs

def opt():
    opts = rocksdb.Options()
    opts.create_if_missing = True
    opts.max_open_files = 30
    opts.write_buffer_size = 50*1024*1024
    opts.max_write_buffer_number = 3
    opts.target_file_size_base = 67108864    
    opts.compression = rocksdb.CompressionType.zlib_compression# rocksdb.CompressionType.no_compression
    opts.delete_obsolete_files_period_micros = 1000000 * 10
    opts.keep_log_file_num = 10
    opts.allow_mmap_reads = True
    opts.allow_mmap_writes = True
    opts.min_write_buffer_number_to_merge = 2

    opts.table_factory = rocksdb.BlockBasedTableFactory(checksum='xxhash', filter_policy=rocksdb.BloomFilterPolicy(10),
    block_cache=rocksdb.LRUCache(2 * (1024 ** 3)), block_size=1024*512,
    block_cache_compressed=rocksdb.LRUCache(500 * (1024 ** 2)))

    return opts