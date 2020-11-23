import rocksdb
import os
import traceback

max_map_size = (1073741824*1024)
small_block_db_path = os.path.join(os.getcwd(), '..', '..', 'metadata', "small_blocks")


def write_small_block(_hash, data, stat_msg_queue):
    smbs = rocksdb.DB(small_block_db_path, opt())
    smbs.put(_hash.encode(), bytes(data))

    return len(data)


def read_small_block(_hash, stat_msg_queue):    
    smbs = rocksdb.DB(small_block_db_path, opt())

    return smbs.get(_hash.encode())
        

def delete_small_block(_hash, stat_msg_queue):    
    smbs = rocksdb.DB(small_block_db_path, opt())
    try:
        smbs.delete(_hash.encode())
        return True
    except:
        print(traceback.format_exc())
        return False