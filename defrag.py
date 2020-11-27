from stats import Timer
from datastructures import DataStore, Block, FileBlock
from hashtable import HashTable
from BTrees import IOBTree

def defrag(free_blocks:IOBTree, hash_table:HashTable, datastore:DataStore):
    '''
    param free_blocks: BTree IOBTree with all blocks that are free
    param hash_table: HashTable with 
    '''
    
    timer = Timer()
    timer.start()    
    print("Starting Defrag - Expect some slowdown")
    

    