from fastcdc import fastcdc  #  list(fastcdc(d1, 4096, 8192, 16384))
import xxhash
import hashlib

from configurations import min_blk_size, mean_blk_size, max_blk_size

def hashed_chunks(data, fat=True, hf=xxhash.xxh3_64):
    return list(fastcdc(data, min_blk_size, mean_blk_size, max_blk_size, fat=fat, hf=hf))


def hash_data(data):
    return xxhash.xxh3_64(data).hexdigest()

# def hash_data(_data):
#     """
#     Takes the raw data and calculates it's hash, returning the hexdigest (hex without the leading charecters x0)
#     :param _data: data to be hashed
#     :return: hexdigest (hex without the leading charecters x0)
#     """
#     # return hashlib.sha3_256(_data).hexdigest()
#     # return hashlib.sha3_512(_data).hexdigest()
#     return hashlib.sha1(_data).hexdigest()
