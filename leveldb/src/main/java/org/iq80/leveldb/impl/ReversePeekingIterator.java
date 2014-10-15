
package org.iq80.leveldb.impl;

import com.google.common.collect.PeekingIterator;

public interface ReversePeekingIterator<T> extends ReverseIterator<T>, PeekingIterator<T>
{
   T peekPrev();
}


