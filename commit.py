import mmap
from hashing import hash_data
import traceback
import time

def final_commit(work):    
    for data in work:
        try:
            # start = time.time()
            with open(data[1][data[0].chunk].path, "r+b") as f:
                mm = mmap.mmap(f.fileno(), length=data[1][data[0].chunk].size, access=mmap.ACCESS_WRITE)
                mm.seek(data[0].block)
                if data[0].compressed:                            
                    written = mm.write(data[0].compressed_data)
                    written_hash = hash_data(data[0].compressed_data)
                else:                            
                    written = mm.write(data[0].data)
                    written_hash = hash_data(data[0].data)

                if not data[0].compressed and written_hash != data[0].hash:
                    print("Data corruption - Hash inconsistance")

                elif data[0].compressed and written_hash != hash_data(data[0].compressed_data):
                    print("Data corruption - Hash inconsistance")
                # mm.flush()
                # mm.close()
                # stat_msg_queue.put_nowait({'INFO:WriteSpeed' : written/(time.time()-start)})
                # print("written "+ str(written))
                # return True
        except:
            print(traceback.format_exc())
                # return False 
    # print(time.time())
    return True