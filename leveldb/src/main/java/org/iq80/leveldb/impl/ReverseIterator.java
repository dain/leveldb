
package org.iq80.leveldb.impl;

import java.util.Iterator;

public interface ReverseIterator<T> extends Iterator<T>
{
   T prev();
   boolean hasPrev();
}


