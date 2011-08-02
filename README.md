# LevelDB in Java

This is a rewrite (port) of [LevelDB](http://code.google.com/p/leveldb/) in
Java.  This goal is to have a feature complete implementation that is within
10% of the performance of the C++ original and produces byte-for-byte exact
copies of the C++ code.

# Current status

Currently the code base is basically functional, but only trivially tested.
This code some places the code is a literal conversion of the C++ code and in
others it has been converted to a more natural Java style.  The plan is to
leave the code closer to the C++ original until the baseline performance has
been established.


## DB implementation

* Get, put, delete, batch writes and iteration implemented
* Snapshots implemented (needs testing)

## Storage

* MemTables implemented
* Read and write tables
* Read and write blocks
* Supports Snappy compression
* Supports CRC32c checksums

## Compaction

* MemTable to Level0 compaction
* Version persistence and VersionSet management
* Read and write log files

# Implementation Nodes

## Iterators

The iterator intensive design of this code comes directly from the C++ code.
LevelDB can most easily described follows:

* DB merge Iterator
 * MemTable iterator
 * Immutable MemTable iterator (the one being compacted)
 * Version merge iterator
  * Level0 merge iterator over files
   * Table merge iterator
    * Block iterator
  * Level1 concat iterator over files
   * Table merge iterator
    * Block iterator
  * ...
  * LevelN concat iterator over files
   * Table merge iterator
    * Block iterator

As you can see it is easy to get lost in these deeply nested data structures.
In addition to these iterators from the original C++ code, this code wraps the
DB  merge iterator with a snapshot filtering iterator and finally a
transforming iterator to convert InternalKeys into the keys in the user space.

## Buffers

Currently the code uses Netty ChannelBuffers internally.  This is mainly
because the Java ByteBuffer interface is so unfriendly.  ChannelBuffers
are not really ideal for this code, either and a custom solution needs to be
considered

## Thread safety

None of the locking code from the original C++ was translated into Java largely
because Java and C++ concurrent primitives and data structures are so different.
Once the compaction is in place it should be more clear how to make the DB
implementation thread safe and concurrent.

## Memory usage

Since the code is a fairly literal translation of the original C++, the memory
usage is more restrained that most Java code, but further work need to be done
around clean up of abandoned user objects (like Snapshots).  The code also
sometimes makes extra copies of buffers due to the ChannelBuffer, ByteBuffer
and byte[] impedance.  Over time, the code should be tuned to reduce GC impact
(the most problematic code is the skip list in the memtable which may need to
be rewritten).  Of course, all of this must be verified in a profiler.

# TODO

## Compaction

* Level0 compaction
* Arbitrary range compaction
* Compaction scheduling

## Performance

There has been no performance tests yet.

* Port C++ performance benchmark to Java
* Establish performance base line against:
 * C++ original
 * Kyoto TreeDB
 * SQLite3
 * [LevelDB JNI] (https://github.com/fusesource/leveldbjni)

## API

The user APIs have not really been started yet, but there are a few ideas on
the drawing-board already.

* Factory/maker API for opening and creating databases (like Guava)
* Low-level simple buffer only API
* High-level java.util.Map like or full map wrapper with serialization support
* SPI for UserComparator

## Other

* Need logging interface
* TX logging and recovery
* Need iterator structure inspector for easier debugging
*   All buffers must be in little endian
