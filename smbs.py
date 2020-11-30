import rocksdb
import traceback
from utils import opt, load_configuration

confs = load_configuration()

small_block_db_path = confs['small_block_db_path']

def write_small_block(_hash, data, mirror_datastore, stat_msg_queue):
    smbs = rocksdb.DB(small_block_db_path, opt())
    smbs.put(_hash.encode(), bytes(data))

    return len(data)


def read_small_block(_hash, stat_msg_queue):    
    smbs = rocksdb.DB(small_block_db_path, opt())

    return smbs.get(_hash.encode())
        

def delete_small_block(_hash, mirror_datastore, stat_msg_queue):
    smbs = rocksdb.DB(small_block_db_path, opt())
    try:
        smbs.delete(_hash.encode())
        return True
    except:
        print(traceback.format_exc())
        return False