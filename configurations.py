from collections import deque
from math import ceil
from cache import LRU
from compression import compressed_pickle, decompress_pickle, decompress_data, compress_data
from datastructures import Garbage_Collector
from hashtable import HashTable
from utils import load_configuration

confs = load_configuration()

datastore = []
hash_table = HashTable()
fragmentation = {}
fragmentation['free_size'] = 0
fragmentation['free_count'] = 0

free_blocks = []
GC = Garbage_Collector()

usage_interval = confs['usage_interval']
gc_interval = confs['gc_interval']

dirs = {}
write_read_cache = {}
write_buffer = deque()

read_cache = LRU(maxlen=confs['cache_size'])
small_block_read_cache =LRU(maxlen=confs['small_block_cache_size'])

allocation_unit = confs['allocation_unit']

mean_blk_size = allocation_unit*confs['mean_blk_size']
min_blk_size = mean_blk_size//4
max_blk_size = mean_blk_size * 8
small_block_limit = allocation_unit*confs['small_block_limit']    # Any block that after compacted is smaller than this will be stored in memory and persisted by ZODB

partition_size = confs['partition_size']  # Partion size in GB
partition_size_gb = partition_size * 1073741824
ds_size = partition_size * 1073741824
chunk_size_per_GB = confs['chunk_size_per_GB']
chunk_size = int(ceil(chunk_size_per_GB * 1073741824))
datastore_chunks_number = int(ceil(ds_size/chunk_size))  # DataStore chunks equal to one every GB of partition size

fs_meta_path = confs['fs_meta_path']
hash_table_path = confs['hash_table_path']
free_blocks_path = confs['free_blocks_path']
gc_path = confs['gc_path']
small_block_db_path = confs['small_block_db_path']
datastore_base_path = confs['datastore_base_path']

datastore_chunks = list(range(0, datastore_chunks_number))

identity_string = confs['identity_string']

backend = confs['backend']
write_spread = confs['write_spread']

max_write_workers = confs['max_write_workers'] + 9 # 9 is the number of threads that are constantly running without consider the write threads

q_random = confs['q_random'] # Maximum number of the random range to the tested against to decided if one of the spammy messages will make to the queue