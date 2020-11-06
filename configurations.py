from collections import deque
from BTrees import IOBTree, OOBTree
from math import ceil
from cache import LRU
from compression import compressed_pickle, decompress_pickle, decompress_data, compress_data
import os
import time
from datastructures import Garbage_Collector

"""
Bunch of stuff to test the concept. Change this shit later to something useful fast and safe
"""
datastore = []
key_index = {}
hash_table = {}
fs_meta = None
fragmentation = {}
fragmentation['free_size'] = 0
fragmentation['free_count'] = 0

performance_measure_bars = False

# Blocks = []
# NEXT_BLOCK_OFFSET = []
free_blocks = []
GC = Garbage_Collector()

inodes = {} #IOBTree.IOBTree()
dirs = {}
# contents = {} #OOBTree.OOBTree() #IOBTree.IOBTree()

# Buffer de escrita em multiplos da unidade de alocacao
# sendo assim os arquivos serão persistidos a cada X blocos/unidades de alocacao, sendo X o write_buffer_size ou a cada
# Y segundos, sendo Y o write_buffer_lifetime
write_read_cache = {}
write_buffer = deque()  # queue.Queue()
write_buffer_size = 3000
write_buffer_lifetime = 15
write_buffer_lock = False
gc_interval = 20  # Intervalo entre o final de uma operação de GC e o inicio de outra
usage_interval = 5 # Time in seconds to update the usage couters

over_fetch_limit = 6  # 64 blocks of 16k = 1MB
over_read_limit = 0 #2097152*10  # Number of bytes that will be read at each interaction of the read loop. This is effectivily a cache
cache_size = 1  # Cache size in entries. Memory size is ~cache_size*allocation_unit
read_cache = LRU(maxlen=cache_size)
small_block_cache_size = 1
small_block_read_cache =LRU(maxlen=small_block_cache_size)

dummy_mult = 1024
dummy_allocation_unit = 1024*dummy_mult
allocation_unit = 1024
min_blk_size = allocation_unit*16
mean_blk_size = allocation_unit*32
max_blk_size = allocation_unit*64
small_block_limit = min_blk_size // 4    # Any block that after compacted is smaller than this will be stored in memory and persisted by ZODB
block_negative_limit = -4
deletion_grace_period = 60

partition_size = 80  # Partion size in GB
partition_size_gb = partition_size * 1073741824
ds_size = partition_size * 1073741824
read_allocation_unit = 1024*128
chunk_size_per_GB = 4
chunk_size = int(ceil(chunk_size_per_GB * 1073741824))
datastore_chunks_number = int(ceil(ds_size/chunk_size))  # DataStore chunks equal to one every GB of partition size
max_write_threads = datastore_chunks_number # Sets the maximum number of writing threads to the number of chunks

key_index_path = os.path.join(os.getcwd(), '..', '..', 'metadata', "key_index.bin")
fs_meta_path = os.path.join(os.getcwd(), '..', '..', 'metadata', "fs.meta")
hash_table_path = os.path.join(os.getcwd(), '..', '..', 'metadata', "hash_table.bin")
free_blocks_path = os.path.join(os.getcwd(), '..', '..', 'metadata', "free_blocks.bin")
gc_path = os.path.join(os.getcwd(), '..', '..', 'metadata', "gc.vfs")
small_block_db_path = os.path.join(os.getcwd(), '..', '..', 'metadata', "small_blocks.sqlite")

datastore_base_path = os.path.join(os.getcwd(), '..', '..', 'metadata')
datastore_chunks = list(range(0, datastore_chunks_number))

identity_string = b'VeratyFS@v0.0.1@InLineDedup,FixedStoreSize,GC,Compression,FixedBlockSize\n'

q_random = 2 # Maximum number of the random range to the tested against to decided if one of the spammy messages will make to the queue

smb_write_threads = 1
bb_write_threads = 2

writer_thread_mem_limit = 1073741824 * 2