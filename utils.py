from bisect import bisect_left, bisect_right
from pathlib import Path
import rocksdb
import yaml
import traceback
import os


def take_closest(ordList, myNumber, left=True):
    """
    Assumes myList is sorted. Returns closest value to myNumber.

    If two numbers are equally close, return the smallest number.

    Returns (element,position)
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

def load_configuration():
    try:
        config_path = os.path.join(os.getcwd(), 'config.yaml')
        with open(config_path, 'r') as stream:
            return yaml.safe_load(stream)
    except:
        print("Could not load configurations. Please check 'config.yaml' to ensure it has the proper configurations.")
        print(traceback.format_exc())
        return None

def opt():

    '''    
    Recomended Defaults:
    Source: https://github.com/facebook/rocksdb/wiki/Setup-Options-and-Basic-Tuning
    cf_options.level_compaction_dynamic_level_bytes = true;
    options.max_background_compactions = 4;
    options.max_background_flushes = 2;
    options.bytes_per_sync = 1048576;
    options.compaction_pri = kMinOverlappingRatio;
    table_options.block_size = 16 * 1024;
    table_options.cache_index_and_filter_blocks = true;
    table_options.pin_l0_filter_and_index_blocks_in_cache = true;    
    '''

    opts = rocksdb.Options()
    opts.create_if_missing = True
    opts.max_open_files = 30
    opts.write_buffer_size = 50*1024*1024
    opts.max_write_buffer_number = 300
    opts.target_file_size_base = 67108864    
    opts.compression = rocksdb.CompressionType.zlib_compression# rocksdb.CompressionType.no_compression
    opts.delete_obsolete_files_period_micros = 1000000 * 10
    opts.keep_log_file_num = 1
    opts.allow_mmap_reads = True
    opts.allow_mmap_writes = True
    # opts.manual_wal_flush = True # TODO: RE-ENABLE THIS AS IT GIVES GOOD PERFORMANCE IMPROVEMENT. TAKE CARE TO **MANUALLY** FLUSH ALL DATA
    # opts.use_direct_reads = True
    # opts.use_direct_io_for_flush_and_compaction = True
    opts.min_write_buffer_number_to_merge = 2
    opts.avoid_unnecessary_blocking_io = True
    opts.two_write_queues = True
    opts.unordered_write= True
    opts.max_background_jobs = 2
    opts.level_compaction_dynamic_level_bytes = True
    opts.max_background_compactions = 10
    opts.max_background_flushes = 2
    opts.bytes_per_sync = 1048576
    opts.compaction_pri = rocksdb.CompactionPri().min_overlapping_ratio
    
    
    opts.table_factory = rocksdb.BlockBasedTableFactory(
    checksum='xxhash',
    filter_policy=rocksdb.BloomFilterPolicy(10),
    block_cache=rocksdb.LRUCache(0.5 * (1024 ** 3)),
    block_size=16*1024,
    block_cache_compressed=rocksdb.LRUCache(0.5 * (1024 ** 2)),
    cache_index_and_filter_blocks=True)

    return opts

def datastore_opt():    

    opts = rocksdb.Options()
    opts.create_if_missing = True
    opts.max_open_files = 30000
    opts.write_buffer_size = (0.3 * (1024 ** 3))
    opts.max_write_buffer_number = 5
    opts.target_file_size_base = (128 * (1024 ** 2))
    opts.compression = rocksdb.CompressionType.no_compression
    # opts.delete_obsolete_files_period_micros = 1000000 * 60
    opts.keep_log_file_num = 2
    opts.allow_mmap_reads = True
    opts.allow_mmap_writes = True
    # opts.manual_wal_flush = True # TODO: RE-ENABLE THIS AS IT GIVES GOOD PERFORMANCE IMPROVEMENT. TAKE CARE TO **MANUALLY** FLUSH ALL DATA
    # opts.use_direct_reads = True
    # opts.use_direct_io_for_flush_and_compaction = True
    opts.min_write_buffer_number_to_merge = 2
    opts.avoid_unnecessary_blocking_io = True
    opts.two_write_queues = True
    opts.unordered_write= True
    opts.max_background_jobs = 4
    opts.level_compaction_dynamic_level_bytes = True
    opts.max_background_compactions = 4
    opts.max_background_flushes = 4
    opts.bytes_per_sync = 1048576*10
    opts.compaction_pri = rocksdb.CompactionPri().min_overlapping_ratio
    
    
    opts.table_factory = rocksdb.BlockBasedTableFactory(
    checksum='xxhash',
    filter_policy=rocksdb.BloomFilterPolicy(10),
    block_cache=rocksdb.LRUCache(0.2 * (1024 ** 3)),
    block_size=(256)*1024,
    block_cache_compressed=rocksdb.LRUCache(0.2 * (1024 ** 3)),
    cache_index_and_filter_blocks=True)

    return opts

#TODO: Check on how to eleminate the .fuse_hidden files 
def get_dir_size(path:str) -> int:
    try:
        root_directory = Path(path)
        return sum(f.stat().st_size for f in root_directory.glob('**/*') if f.is_file() and not f.name.startswith('.'))
    except FileNotFoundError:
        return 0


def define_chunks(datastore_chunks:list, datastore_base_path:list, backend:str) -> list:
    if backend == 'mmap':
        name = 'chunk'
        suffix = 'bin'        
    else:
        suffix = ''
        name = 'store'

    stores = []
    current_store_path = 0
    for chunk in datastore_chunks:
        stores.append(os.path.join(datastore_base_path[current_store_path], name + str(chunk) + suffix))
        current_store_path = current_store_path + 1
        if current_store_path == len(datastore_base_path):
            current_store_path = 0

        if not os.path.isdir(stores[-1]):
            os.makedirs(stores[-1])

    return stores