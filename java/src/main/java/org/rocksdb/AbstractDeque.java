// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

import java.util.Deque;
import java.util.Collection;
import java.util.Iterator;

/**
 * A Java deque backed by a JNI c++ deque.
 * 
 * This class is package private, implementors
 * should extend either of the public abstract classes:
 *   @see org.rocksdb.ByteArrayDeque
 *   @see org.rocksdb.SliceDeque
 *   @see org.rocksdb.DirectSliceDeque
 */
abstract class AbstractDeque<E> extends RocksObject implements Deque<E> {
  
  @Override public abstract void addFirst(E e);

  @Override public abstract void addLast(E e);

  @Override public boolean offerFirst(E e) {
    addFirst(e);
    return true;
  }

  @Override public boolean offerLast(E e) {
    addLast(e);
    return true;
  }

  @Override public abstract E removeFirst();

  @Override public abstract E removeLast();

  @Override public abstract E pollFirst();

  @Override public abstract E pollLast();

  @Override public abstract E getFirst();

  @Override public abstract E getLast();

  @Override public abstract E peekFirst();

  @Override public abstract E peekLast();

  @Override public abstract boolean removeFirstOccurrence(Object o);

  @Override public abstract boolean removeLastOccurrence(Object o);

  @Override public boolean add(E e) {
    addLast(e);
    return true;
  }

  @Override public boolean offer(E e) {
    offerLast(e);
    return true;
  }

  @Override public E remove() {
    return removeFirst();
  }

  @Override public E poll() {
    return pollFirst();
  }

  @Override public E element() {
    return getFirst();
  }

  @Override public E peek() {
    return peekFirst();
  }

  @Override public void push(E e) {
    addFirst(e);
  }

  @Override public E pop() {
    return removeFirst();
  }

  @Override public abstract boolean remove(Object o);

  @Override public abstract boolean containsAll(Collection<?> c);

  @Override public abstract boolean addAll(Collection<? extends E> c);

  @Override public abstract boolean removeAll(Collection<?> c);

  @Override public abstract boolean retainAll(Collection<?> c);

  @Override public void clear() {
    assert (isInitialized());
    clear0(nativeHandle_);
  }

  @Override public abstract boolean contains(Object o);

  @Override public int size() {
    assert (isInitialized());
    return size0(nativeHandle_);
  }

  @Override public boolean isEmpty() {
    assert (isInitialized());
    return isEmpty0(nativeHandle_);
  }

  @Override public abstract Iterator<E> iterator();

  @Override public abstract Object[] toArray();

  @Override public abstract <T> T[] toArray(T[] a);

  @Override public abstract Iterator<E> descendingIterator();

  /**
   * Creates a string representation of the data
   *
   * @param hex When true, the representation
   *   will be encoded in hexadecimal.
   *
   * @return The string representation of the data.
   */
  public String toString(final boolean hex) {
    assert (isInitialized());
    return toString0(nativeHandle_, hex);
  }

  @Override
  public String toString() {
    return toString(false);
  }

  /**
   * Deletes underlying C++ slice pointer.
   * Note that this function should be called only after all
   * RocksDB instances referencing the slice are closed.
   * Otherwise an undefined behavior will occur.
   */
  @Override
  protected void disposeInternal() {
    assert(isInitialized());
    disposeInternal(nativeHandle_);
  }

  private native int size0(long handle);
  private native boolean isEmpty0(long handle);
  private native String toString0(long handle, boolean hex);
  private native void clear0(long handle);
  private native void disposeInternal(long handle);

}
