# LevelDB in Java

This is a rewrite (port) of [LevelDB](http://code.google.com/p/leveldb/) in
Java.  This goal is to have a feature complete implementation that is within
10% of the performance of the C++ original and produces byte-for-byte exact
copies of the C++ code.

# Current status

Currently the code base is basically functional, but only trivially tested.
In some places, this code is a literal conversion of the C++ code and in
others it has been converted to a more natural Java style.  The plan is to
leave the code closer to the C++ original until the baseline performance has
been established.

## API Usage:

Recommended Package imports:

```java
import org.iq80.leveldb.*;
import static org.iq80.leveldb.impl.Iq80DBFactory.*;
import java.io.*;
```

Opening and closing the database.

```java
Options options = new Options();
options.createIfMissing(true);
DB db = factory.open(new File("example"), options);
try {
  // Use the db in here....
} finally {
  // Make sure you close the db to shutdown the 
  // database and avoid resource leaks.
  db.close();
}
```

Putting, Getting, and Deleting key/values.

```java
db.put(bytes("Tampa"), bytes("rocks"));
String value = asString(db.get(bytes("Tampa")));
db.delete(bytes("Tampa"), wo);
```

Performing Batch/Bulk/Atomic Updates.

```java
WriteBatch batch = db.createWriteBatch();
try {
  batch.delete(bytes("Denver"));
  batch.put(bytes("Tampa"), bytes("green"));
  batch.put(bytes("London"), bytes("red"));

  db.write(batch);
} finally {
  // Make sure you close the batch to avoid resource leaks.
  batch.close();
}
```

Iterating key/values.

```java
DBIterator iterator = db.iterator();
try {
  for(iterator.seekToFirst(); iterator.hasNext(); iterator.next()) {
    String key = asString(iterator.peekNext().getKey());
    String value = asString(iterator.peekNext().getValue());
    System.out.println(key+" = "+value);
  }
} finally {
  // Make sure you close the iterator to avoid resource leaks.
  iterator.close();
}
```

Working against a Snapshot view of the Database.

```java
ReadOptions ro = new ReadOptions();
ro.snapshot(db.getSnapshot());
try {
  
  // All read operations will now use the same 
  // consistent view of the data.
  ... = db.iterator(ro);
  ... = db.get(bytes("Tampa"), ro);

} finally {
  // Make sure you close the snapshot to avoid resource leaks.
  ro.snapshot().close();
}
```

Using a custom Comparator.

```java
DBComparator comparator = new DBComparator(){
    public int compare(byte[] key1, byte[] key2) {
        return new String(key1).compareTo(new String(key2));
    }
    public String name() {
        return "simple";
    }
    public byte[] findShortestSeparator(byte[] start, byte[] limit) {
        return start;
    }
    public byte[] findShortSuccessor(byte[] key) {
        return key;
    }
};
Options options = new Options();
options.comparator(comparator);
DB db = factory.open(new File("example"), options);
```
    
Disabling Compression

```java
Options options = new Options();
options.compressionType(CompressionType.NONE);
DB db = factory.open(new File("example"), options);
```

Configuring the Cache

```java    
Options options = new Options();
options.cacheSize(100 * 1048576); // 100MB cache
DB db = factory.open(new File("example"), options);
```

Getting approximate sizes.

```java
long[] sizes = db.getApproximateSizes(new Range(bytes("a"), bytes("k")), new Range(bytes("k"), bytes("z")));
System.out.println("Size: "+sizes[0]+", "+sizes[1]);
```
    
Getting database status.

```java
String stats = db.getProperty("leveldb.stats");
System.out.println(stats);
```

Getting informational log messages.

```java
Logger logger = new Logger() {
  public void log(String message) {
    System.out.println(message);
  }
};
Options options = new Options();
options.logger(logger);
DB db = factory.open(new File("example"), options);
```

Destroying a database.

```java    
Options options = new Options();
factory.destroy(new File("example"), options);
```

# Projects using this port of LevelDB

* [ActiveMQ Apollo](http://activemq.apache.org/apollo/): Defaults to using leveldbjni, but falls 
  back to this port if the jni port is not available on your platform.
