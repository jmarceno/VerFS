from collections import deque
from BTrees import IOBTree
from math import ceil
from cache import LRU
from compression import compressed_pickle, decompress_pickle, decompress_data, compress_data
import os
import time

class MyBTree(IOBTree.BTree):
    max_leaf_size = 500
    max_internal_size = 1000

class Garbage_Collector:
    def __init__(self):
        self.add_uses = deque()  # Hash of blocks that should receive an additional use counter
        self.remove_uses = deque() # Hash of blocks that should have their uses counter decreased

class DataStore:
    def __init__(self, _chunk, _chunk_size, _path):
        self.chunk = _chunk
        self.size = _chunk_size
        self.path = _path
        self.next_write_position = 0
        self.IS_FULL = False


class QueuedWrite:
    def __init__(self, _idx, _hash, _data):
        self.idx = _idx
        self.hash = _hash
        self.data = _data
        self.compressed_data = self.__compress__()
        self.chunk = 0
        self.block = 0
        self.result = False
        self.creation_time = time.time()
        self.compressed = False        

        if len(self.compressed_data) != len(self.data):
            self.compressed = True

    def __compress__(self):
        compressed, d = compress_data(self.data)
        # d = len(d).to_bytes(2, byteorder='little') + d

        return d

class Block:
    def __init__(self):
        self.chunk = 0
        self.offset = 0  # Offset dentro do chunk
        self.hash = ""
        self.size = 0  # Tamanho depois da compressao
        self.deflated_size = 0  # Tamanho sem compressao
        self.uses = 1
        self.compressed = False
        self.DELETED = False
        self.DELETION_TIME = time.time()


class FileBlock:
    def __init__(self, _hash, _size):
        self.hash = _hash
        self.size = _size

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
cache_size = 50000  # Cache size in entries. Memory size is ~cache_size*allocation_unit
read_cache = LRU(maxlen=cache_size)
header_size = 2
block_address_size = 5

min_blk_size = 1024*32
mean_blk_size = 1024*64
max_blk_size = 1024*128

partition_size = 4  # Partion size in GB
ds_size = partition_size * 1073741824
allocation_unit = 16384
read_allocation_unit = 1024*128
chunk_size_per_GB = 4
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