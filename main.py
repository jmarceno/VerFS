import argparse

from VeratyFS import main

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("mountpoint")
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("-d", "--debug", action="store_true")
    parser.add_argument("-l", "--label", type=str, default="VeratyFS")
    parser.add_argument("-m", "--meta", type=str, default=None)
    parser.add_argument("-t", "--datastore", type=str, default=None)
    parser.add_argument("-s", "--size", type=int, default=4)  # Size in GB
    parser.add_argument("-k", "--keys", type=str, default=None)
    parser.add_argument("-a", "--hash", type=str, default=None)
    parser.add_argument("-f", "--free_blocks", type=str, default=None)
    parser.add_argument("-u", "--allocation_unit", type=int, default=16384)  # Allocation Unit in Bytes
    parser.add_argument("-c", "--compression_type", type=str, default=None)
    args = parser.parse_args()
    main(args.mountpoint, args.label, args.verbose, args.debug, args.meta, args.datastore, args.size, args.hash,
         args.free_blocks, args.keys, args.allocation_unit, args.compression_type)
