def write_small_block(_hash, data, stat_msg_queue, ds_path):    
    # if not os.path.exists(directory):
    #     try:
    #         os.makedirs(directory)
    #     except FileExistsError:
    #         pass    
    
    # with SqliteDict(ds_path) as smbs:  # note no autocommit=True
    #     smbs[_hash] = data
    #     smbs.commit()

    # del smbs

    env = lmdb.open(ds_path, max_dbs=0, map_size=chunk_size)
    with env.begin(write=True) as txn:
        try:
            txn.put(_hash.encode(), data)
            txn.commit()            
        except:
            print(traceback.format_exc())
            return 0
    
    return len(data)

def read_small_block(_hash, stat_msg_queue, ds_path):    
    # with SqliteDict(ds_path) as smbs:  # note no autocommit=True
    #     try:
    #         return smbs[_hash]
    #     except:
    #         return False
    try:
        env = lmdb.open(ds_path, max_dbs=0, map_size=chunk_size)
        with env.begin() as txn:
            with txn.cursor() as curs:
                return txn.get(_hash.encode())
    except:
        print(traceback.format_exc())
        return False
    
    # try:
    #     env = lmdb.open(ds_path, max_dbs=0, map_size=chunk_size)
    #     with env.begin(write=False) as txn:            
    #         return txn.get(_hash.encode())
    # except:
    #     print(traceback.format_exc())
    #     return False

def delete_small_block(_hash, stat_msg_queue, ds_path):
    try:
        env = lmdb.open(ds_path, max_dbs=0, map_size=chunk_size)
        with env.begin(write=True) as txn:
            txn.delete(_hash.encode())
            return True
    except:
        print(traceback.format_exc())
        return False
    # with SqliteDict(ds_path) as smbs:  # note no autocommit=True
    #     try:
    #         del smbs[_hash]
    #         smbs.commit()
    #         return True
    #     except:
    #         print(traceback.format_exc())
    #         return False