
from bisect import bisect_left, bisect_right

import rocksdb
import yaml
import traceback

def take_closest(ordList, myNumber, left=True):
    """
    Assumes myList is sorted. Returns closest value to myNumber.

    If two numbers are equally close, return the smallest number.
    """
    if left:
        pos = bisect_left(ordList, myNumber)
    else:
        pos = bisect_right(ordList, myNumber)

    if pos == 0:
        return ordList[0], pos
    if pos == len(ordList):
        return ordList[-1], pos-1
    # before = myList[pos - 1]
    return ordList[pos], pos
    # after = myList[pos]
    # if after - myNumber < myNumber - before:
    #    return after, pos
    # else:
    #    return before, pos-1


def opt():
    opts = rocksdb.Options()
    opts.create_if_missing = True
    opts.max_open_files = 3000
    opts.write_buffer_size = 50*1024*1024
    opts.max_write_buffer_number = 3000
    opts.target_file_size_base = 67108864    
    opts.compression = rocksdb.CompressionType.snappy_compression #rocksdb.CompressionType.zlib_compression# rocksdb.CompressionType.no_compression
    opts.delete_obsolete_files_period_micros = 1000000 * 10
    opts.keep_log_file_num = 10    
    opts.allow_mmap_reads = True
    opts.allow_mmap_writes = True
    opts.manual_wal_flush = True
    # opts.use_direct_reads = True
    # opts.use_direct_io_for_flush_and_compaction = True
    opts.min_write_buffer_number_to_merge = 2
    opts.avoid_unnecessary_blocking_io = True
    opts.two_write_queues = True
    opts.unordered_write= True
    opts.max_background_jobs = 20
    
    
    opts.table_factory = rocksdb.BlockBasedTableFactory(checksum='xxhash', filter_policy=rocksdb.BloomFilterPolicy(10),
    block_cache=rocksdb.LRUCache(4 * (1024 ** 3)), block_size=1024*4,
    block_cache_compressed=rocksdb.LRUCache(2048 * (1024 ** 2)))

    return opts

def load_configuration():
    try:
        with open("config.yaml", 'r') as stream:        
            return yaml.safe_load(stream)
    except:
        print("Could not load configurations. Please check 'config.yaml' to ensure it has the proper configurations.")
        print(traceback.format_exc())
        return None