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
public abstract class ByteArrayDeque extends AbstractDeque<byte[]> {
  
  @Override public abstract void addFirst(byte[] elem);

  @Override public abstract void addLast(byte[] elem);

  @Override public abstract boolean offerFirst(byte[] elem);

  @Override public abstract boolean offerLast(byte[] elem);

  @Override public abstract byte[] removeFirst();

  @Override public abstract byte[] removeLast();

  @Override public abstract byte[] pollFirst();

  @Override public abstract byte[] pollLast();

  @Override public abstract byte[] getFirst();

  @Override public abstract byte[] getLast();

  @Override public abstract byte[] peekFirst();

  @Override public abstract byte[] peekLast();

  @Override public abstract boolean removeFirstOccurrence(Object o);

  @Override public abstract boolean removeLastOccurrence(Object o);

  @Override public abstract boolean add(byte[] elem);

  @Override public abstract boolean offer(byte[] elem);

  @Override public abstract byte[] remove();

  @Override public abstract byte[] poll();

  @Override public abstract byte[] element();

  @Override public byte[] peek() {
    assert (isInitialized());
    return peek0(nativeHandle_);
  }

  @Override public void push(byte[] elem) {
    assert (isInitialized());
    push0(nativeHandle_, elem);
  }

  @Override public byte[] pop() {
    assert (isInitialized());
    return pop0(nativeHandle_); 
  }

  @Override public abstract boolean remove(Object o);

  @Override public abstract boolean containsAll(Collection<?> c);

  @Override public abstract boolean addAll(Collection<? extends byte[]> c);

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

  @Override public abstract Iterator<byte[]> iterator();

  @Override public abstract Object[] toArray();

  @Override public abstract <T> T[] toArray(T[] a);

  @Override public abstract Iterator<byte[]> descendingIterator();

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
  
  private native byte[] peek0(long handle);
  private native void push0(long handle, byte[] elem);
  private native byte[] pop0(long handle);
  private native int size0(long handle);
  private native boolean isEmpty0(long handle);
  private native String toString0(long handle, boolean hex);
  private native void clear0(long handle);
  private native void disposeInternal(long handle);

}
