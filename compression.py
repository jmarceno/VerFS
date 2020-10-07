import pickle
import bz2
import lzma
import zlib
import _pickle as cPickle
from _bz2 import BZ2Decompressor
import struct

lzma_filters = [
    {"id": lzma.FILTER_DELTA, "dist": 5},
    {"id": lzma.FILTER_LZMA2, "preset": 3 | lzma.MODE_FAST},
]

zlib_compression_level = 6
bz2_compression_level = 9


# Pickle a file and then compress it into a file with extension
def compressed_pickle(path, data, format=1):
    """

    :param path:
    :param data:
    :param format: 1: Bzip2, 2:Zip, 3:Lzma
    """
    format = int(format)
    if format == 1:
        with bz2.BZ2File(path, 'w') as f:
            cPickle.dump(data, f)
    elif format == 2:
        raise NotImplementedError
    elif format == 3:
        raise NotImplementedError


# Load any compressed pickle file
def decompress_pickle(file, format=1):
    """

    :param file:
    :param format:
    :return: 1: Bzip2, 2:Zip, 3:Lzma
    """
    format = int(format)
    if format == 1:
        data = bz2.BZ2File(file, 'rb')
        data = cPickle.load(data)
        return data
    elif format == 2:
        raise NotImplementedError
    elif format == 3:
        raise NotImplementedError


def compress_data(data, format=1):
    """

    :param data:
    :param format:
    :return: 1: Bzip2, 2:Zip, 3:Lzma
    """
    format = int(format)
    if format == 1:
        return bz2.compress(data, bz2_compression_level)
    elif format == 2:
        return zlib.compress(data, zlib_compression_level)
    elif format == 3:
        return lzma.compress(data, filters=lzma_filters)


def decompress_data(data, format=1):
    """

    :param data:
    :param format:
    :return: 1: Bzip2, 2:Zip, 3:Lzma
    """
    format = int(format)
    if format == 1:
        return bz2_decompress(data)
    elif format == 2:
        raise zlib.decompress(data)
    elif format == 3:
        return lzma_decompress(data)


""""
Data Compressors
"""


def lzma_decompress(data):
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


def bz2_decompress(data):
    decompressor = bz2.BZ2Decompressor()

    results = bytearray()
    while not decompressor.eof:
        results += decompressor.decompress(data)  # struct.pack('<B', data)

    return results


