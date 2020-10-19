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

# Blocks = []
# NEXT_BLOCK_OFFSET = []
free_blocks = []
GC = Garbage_Collector()

inodes = {} #IOBTree.IOBTree()
contents = {} #OOBTree.OOBTree() #IOBTree.IOBTree()

# Buffer de escrita em multiplos da unidade de alocacao
# sendo assim os arquivos serão persistidos a cada X blocos/unidades de alocacao, sendo X o write_buffer_size ou a cada
# Y segundos, sendo Y o write_buffer_lifetime
write_read_cache = {}
write_buffer = deque()  # queue.Queue()
write_buffer_size = 3000
write_buffer_lifetime = 15
write_buffer_lock = False
gc_interval = 60  # Intervalo entre o final de uma operação de GC e o inicio de outra

under_fetch_limit = 2
over_fetch_limit = 6  # 64 blocks of 16k = 1MB
over_read_limit = 2097152*10  # Number of bytes that will be read at each interaction of the read loop. This is effectivily a cache
cache_size = 100000  # Cache size in entries. Memory size is ~cache_size*allocation_unit
read_cache = LRU(maxlen=cache_size)
small_block_cache_size = 300000
small_block_read_cache =LRU(maxlen=small_block_cache_size)
header_size = 2
block_address_size = 5

min_blk_size = 1024*32
mean_blk_size = 1024*64
max_blk_size = 1024*128
small_block_limit = min_blk_size // 2    # Any block that after compacted is smaller than this will be stored in memory and persisted by ZODB

partition_size = 4  # Partion size in GB
partition_size_gb = partition_size * 1073741824
ds_size = partition_size * 1073741824
allocation_unit = 1024
read_allocation_unit = 1024*128
chunk_size_per_GB = 0.5
chunk_size = int(ceil(chunk_size_per_GB * 1073741824))
datastore_chunks_number = int(ceil(ds_size/chunk_size))  # DataStore chunks equal to one every GB of partition size

key_index_path = os.path.join(os.getcwd(), 'metadata', "key_index.vfs")
fs_meta_path = os.path.join(os.getcwd(), 'metadata', "fs.meta")
hash_table_path = os.path.join(os.getcwd(), 'metadata', "hash_table.bin")
free_blocks_path = os.path.join(os.getcwd(), 'metadata', "free_blocks.bin")
gc_path = os.path.join(os.getcwd(), 'metadata', "gc.vfs")

datastore_base_path = os.path.join(os.getcwd(), 'metadata')
datastore_chunks = list(range(0, datastore_chunks_number))

identity_string = b'VeratyFS@v0.0.1@InLineDedup,FixedStoreSize,GC,Compression,FixedBlockSize\n'