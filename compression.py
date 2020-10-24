import pickle
import bz2
import lzma
import zlib
from lz4 import frame
import _pickle as cPickle
from _bz2 import BZ2Decompressor
import struct
import traceback
from copy import deepcopy
import os

lzma_filters = [
    {"id": lzma.FILTER_DELTA, "dist": 5},
    {"id": lzma.FILTER_LZMA2, "preset": 3 | lzma.MODE_FAST},
]

zlib_compression_level = 6
bz2_compression_level = 1
lz4_compression_level = 1 # frame.COMPRESSIONLEVEL_MINHC

compression_trigger = 0.8  # How much the data has to be compressed for it to be worth. Rates below that will cause the data to not be compressed


# Pickle a file and then compress it into a file with extension
def compressed_pickle(path, data, format=1, stat_msg_queue=None):
    """

    :param path:
    :param data:
    :param format: 1: Bzip2, 2:Zip, 3:Lzma, 4:No-Compression
    """
    format = int(format)
    if format == 1:        
        if type(data) == dict:
            n_data = { k : v for k,v in data.items() if v}
        else:
            n_data = data
        try:
            with bz2.BZ2File(path, 'w') as f:
                cPickle.dump(n_data, f)
                return True
        except RuntimeError:
            return False
        
    elif format == 2:
        raise NotImplementedError
    elif format == 3:
        raise NotImplementedError
    elif format == 4:
        if type(data) == dict:
            n_data = { k : v for k,v in data.items() if v}
        else:
            n_data = data        
        try:
            with os.open(path, 'w') as f:
                cPickle.dump(n_data, f)
                return True
        except RuntimeError:
            return False



# Load any compressed pickle file
def decompress_pickle(_file, format=1, stat_msg_queue=None):
    """

    :param file:
    :param format:
    :return: 1: Bzip2, 2:Zip, 3:Lzma, 4: No-Compression
    """
    format = int(format)
    if format == 1:
        data = bz2.BZ2File(_file, 'rb')
        return cPickle.load(data)
    elif format == 2:
        raise NotImplementedError
    elif format == 3:
        raise NotImplementedError
    elif format == 4:
        with os.open(_file, mode='r') as f:
            data = f.read()
            return cPickle.load(data)


def compress_data(data, _format=1, stat_msg_queue=None):
    """

    :param data:
    :param _format:
    :return: 1: lz4, 2:Zip, 3:Lzma, 4: bz2
    """
    _format = int(_format)
    if _format == 1:
        cp_data = frame.compress(data, compression_level=lz4_compression_level)
        if len(cp_data) < len(data):
            if len(cp_data) / len(data) < compression_trigger:                
                return True, cp_data
            else:
                return False, data
        else:
            return False, data
    elif _format == 2:
        return zlib.compress(data, zlib_compression_level)
    elif _format == 3:
        return lzma.compress(data, filters=lzma_filters)
    elif _format == 4:
        return bz2.compress(data, bz2_compression_level)


def decompress_data(data, _format=1, stat_msg_queue=None):
    """

    :param data:
    :param _format:
    :return: 1: lz4, 2:Zip, 3:Lzma, 4:BZip2
    """
    _format = int(_format)
    if _format == 1:
        try:
            d_context = frame.create_decompression_context()
            d1, b, e = frame.decompress_chunk(d_context, data)
            return d1
        except ValueError:            
            return data
        except RuntimeError:            
            return data
    elif _format == 2:
        raise zlib.decompress(data)
    elif _format == 3:
        return lzma_decompress(data)
    elif _format == 4:
        return bz2_decompress(data, stat_msg_queue)

""""
LZ4 NOTE 
https://python-lz4.readthedocs.io/en/stable/quickstart.html

Note that decompress_chunk() returns a tuple (decompressed_data, bytes_read, end_of_frame_indicator). 
decompressed_data is the decompressed data, bytes_read reports the number of bytes read from the compressed input. 
end_of_frame_indicator is True if the end-of-frame marker is encountered during the decompression, and False otherwise.
If the end-of-frame marker is encountered in the input, no attempt is made to decompress the data after the marker.
"""

"""
Data Compressors
"""


def lzma_decompress(data, stat_msg_queue=None):
    results = []
    while True:
        decomp = lzma.LZMADecompressor(0, None, None)
        try:
            res = decomp.decompress(data)
        except lzma.LZMAError:
            if results:
                break  # Leftover data is not a valid LZMA/XZ stream; ignore it.
            else:
                raise  # Error on the first iteration; bail out.
        results.append(res)
        data = decomp.unused_data
        if not data:
            break
        if not decomp.eof:
            raise lzma.LZMAError("Compressed data ended before the end-of-stream marker was reached")
    return b"".join(results)


def bz2_decompress(data, stat_msg_queue):
    decompressor = bz2.BZ2Decompressor()

    results = bytearray()
    while not decompressor.eof:
        results += decompressor.decompress(data)  # struct.pack('<B', data)

    return results


