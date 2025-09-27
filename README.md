# This is an old experimment - You can read to the code to see how FUSE works but should not use it.

# VeratyFS
VerFS is a userspace block filesystem, with inline de-duplication and compression. Data mirroring and Read/Write spread across multiply volumes/disks, written completely in Python (cPython)

# Motivation
This project started with my fascination to some storage features aimed at reducing storage consumption.
Over time it evolved to study the feasibility of full fledged Posix compatible userspace file system, written completely in Python, incorporating some features, normally reserved to mid/high-end storage systems.  

# Design Principles
Easy to use. No cryptic storage language that would require a lot of training and research to understand. If you can start a python script, you should be able to use it without much trouble.
Sane defaults. The systems defaults have to make sense for the vast majority of use cases in detriment of performance, but not reliability.
As fast and reliable your default Filesystem.

# Requirements
- Linux (with fuse enabled)

- Python 3.9

# How it works
VerFS uses Fuse (https://www.kernel.org/doc/html/latest/filesystems/fuse.html) through one of its Python bindings (https://github.com/libfuse/pyfuse3) to intercept all the calls made to mounted volume previously created with it. Once the OS makes as read/writes request, the call is intercepted and the data pipeline kicks in.
For writes, it split the data in variable size blocks using a technique called FastCDC (https://www.usenix.org/conference/atc16/technical-sessions/presentation/xia), then the data is hashed using xxHash3 64bits (by default, but this can be easily changed in code) and verified against the know hashes and if it matches, if hash matches a new entry is created and data is persisted to the back-end, if hash is already know to the system, it just updated the metadata.
For reads, the whole process happens in reverse with the additional step of "rebuilding" the data from its blocks.
VerFS uses RocksDB (https://github.com/facebook/rocksdb) as the default persistence engine, but a native interface using Python's Memmap is also available.

# Features and Current State
VerFS is currently fully working for some scenarios like data backup or environments where data written once or suffer very few changes or it's lifetime.

 - **Data Read/Write: Working.**

 Please see Issues tab or Know issues section to know more about the current limitations.

 - **Inline De-duplication: Working/Has issues**

 VerFS uses content-aware de-duplication or variable/dynamic block size to achieve very high de-duplication rates (up to 90% for certain scenarios)

 - **Inline Compression: Working/Stable.**

 VerFS was designed to compress all the data that comes in, when it makes sense to, this means that before compressing the data, the system analysis if the gain is big enough to be worth the trouble, as some very small chunks can become bigger if compressed. The default compression method is LZ4, but a few others are available via configuration (gzip, bzip, LZMA), although not recommended as LZ4 has showed almost no performance degradation on the current pipeline.

 - **Data Mirroring: Working/Stable.**

 Any volume can be mirrored to N locations without impacting performance (considering that the media has same throughput on both ends)

 - **Data Spreading: Working/Stable.**

 Any volume can be spread to multiply medias/servers in 2 different configurations. On real-time spread the system uses a simple robin-hood rotation to write the data evenly across different medias/servers and on Contiguous Spread, a new media is used just when the current reaches the defined limit. Both systems have no theoretical limit of spread chunks to be used, so data can be spread to just 2 or to 1000 different medias, although be aware that more chunks, equal more memory needed

- **Admin Console: Working/Needs Revamp**

Today the admin console only shows basic statistics write/read speeds, compression and de-duplication rates and memory used. It needs to be reworked to have access control, better layout and configuration options as all the configuration today is done by text file (.YAML)

- **POSIX Compliance: Not Working/Not Fully Tested**
This needs new testing as the last set of tests was made before some major re-design at the back-end.


# Know Issues

CRITICAL - Mid file writes/updates

The is a bug, that happens when you try to write to block in the middle of a file that is too big for the SO cache (this means the system updates just a certain file offset and not the whole file). The bug makes the systems "lose track" of where the new data should be written in the file. This ONLY happens on updates to a file.
Status - Under investigation, no timeline, PR's welcome.

Important - Memory Usage

- Memory can start to increase to bigger amounts after a certain volume size or queued operations. This happens because the Python interpreter is very conservative releasing the memory once it is allocated and I previously suffered with this in other memory intensive projects. A memory leak at the Cython code is not discarded.
Status - Under investigation, no timeline, PR's welcome.

# Know Limitations

Power outage

One of the main limitation of this completely software driven approach is related to power outages and data-loss/corruption, as the system relies on the server memory and not on dedicated, battery backed RAM, as in some storage controllers.

Volume Size

At this point, just small (<500GB) volumes where tested due to the lack of a proper test environment and the memory issue reported at Know Issues

Performance

As this is a userspace filesystem written in Python...what you thing? :D
