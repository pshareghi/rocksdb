// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

import java.util.Deque;
import java.util.Collection;
import java.util.Iterator;

/**
 * A Java deque, with elements of type DirectSlice, backed by a JNI c++ deque.
 * 
 * 
 * @see org.rocksdb.AbstractDeque
 */
public class DirectSliceDeque extends AbstractDeque<DirectSlice> {
  
  // Used by DirectSliceDequeJni
  public DirectSliceDeque() {
    
  }
  
  @Override public void addFirst(DirectSlice e) {
    assert(isInitialized());
    addFirst0(nativeHandle_, e);
  }

  @Override public void addLast(DirectSlice e) {
    assert(isInitialized());
    addLast0(nativeHandle_, e);
  }

  @Override public DirectSlice removeFirst() {
    assert(isInitialized());
    return removeFirst0(nativeHandle_);
  }

  @Override public DirectSlice removeLast() {
    assert(isInitialized());
    return removeLast0(nativeHandle_);
  }

  @Override public DirectSlice pollFirst() {
    assert(isInitialized());
    return pollFirst0(nativeHandle_);
  }

  @Override public DirectSlice pollLast() {
    assert(isInitialized());
    return pollLast0(nativeHandle_);
  }

  @Override public DirectSlice getFirst() {
    assert(isInitialized());
    return getFirst0(nativeHandle_);
  }

  @Override public DirectSlice getLast() {
    assert(isInitialized());
    return getLast0(nativeHandle_);
  }

  @Override public DirectSlice peekFirst() {
    assert(isInitialized());
    return peekFirst0(nativeHandle_);
  }

  @Override public DirectSlice peekLast() {
    assert(isInitialized());
    return peekLast0(nativeHandle_);
  }

  @Override public boolean removeFirstOccurrence(Object o) {
    assert(isInitialized());
    return removeFirstOccurrence0(nativeHandle_, o);
  }

  @Override public boolean removeLastOccurrence(Object o) {
    assert(isInitialized());
    return removeLastOccurrence0(nativeHandle_, o);
  }

  @Override public boolean remove(Object o) {
    assert(isInitialized());
    return remove0(nativeHandle_, o);
  }

  @Override public boolean containsAll(Collection<?> c) {
    assert(isInitialized());
    return containsAll0(nativeHandle_, c);
  }

  @Override public boolean addAll(Collection<? extends DirectSlice> c) {
    assert(isInitialized());
    return addAll0(nativeHandle_, c);
  }

  @Override public boolean removeAll(Collection<?> c) {
    assert(isInitialized());
    return removeAll0(nativeHandle_, c);
  }

  @Override public boolean retainAll(Collection<?> c) {
    assert(isInitialized());
    return retainAll0(nativeHandle_, c);
  }
  
  @Override public void clear() {
    assert (isInitialized());
    clear0(nativeHandle_);
  }

  @Override public boolean contains(Object o) {
    assert(isInitialized());
    return contains0(nativeHandle_, o);
  }
  
  @Override public int size() {
    assert (isInitialized());
    return size0(nativeHandle_);
  }

  @Override public boolean isEmpty() {
    assert (isInitialized());
    return isEmpty0(nativeHandle_);
  }
  
  @Override public Iterator<DirectSlice> iterator() {
    return new Iter();
  }
  

  @Override public Object[] toArray() {
    assert(isInitialized());
    return toArray0(nativeHandle_);
  }

  @SuppressWarnings("unchecked")
  @Override public <T> T[] toArray(T[] a) {
    int size = size();
    if (a.length < size) {
       a = (T[])java.lang.reflect.Array.newInstance(
           a.getClass().getComponentType(), size);
    }
    
    // a has to be an array with "DirectSlice" elements
    toArray1(nativeHandle_, (Object[]) a);
    
    if (a.length > size) {
      a[size] = null;
    }
    
    return a;
  }

  @Override public Iterator<DirectSlice> descendingIterator() {
    throw new UnsupportedOperationException();
  }
  
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

  /**
   * Deletes underlying C++ deque jni pointer.
   * Note that this function should be called only after all
   * RocksDB instances referencing the deque jni are closed.
   * Otherwise an undefined behavior will occur.
   */
  @Override
  protected void disposeInternal() {
    assert(isInitialized());
    disposeInternal(nativeHandle_);
  }
  
  private native void addFirst0(long handle, DirectSlice e);
  private native void addLast0(long handle, DirectSlice e);
  private native DirectSlice removeFirst0(long handle);
  private native DirectSlice removeLast0(long handle);
  private native DirectSlice pollFirst0(long handle);
  private native DirectSlice pollLast0(long handle);
  private native DirectSlice getFirst0(long handle);
  private native DirectSlice getLast0(long handle);
  private native DirectSlice peekFirst0(long handle);
  private native DirectSlice peekLast0(long handle);
  private native boolean removeFirstOccurrence0(long handle, Object o);
  private native boolean removeLastOccurrence0(long handle, Object o);
  private native boolean remove0(long handle, Object o);
  private native boolean containsAll0(long handle, Collection<?> c);
  private native boolean addAll0(long handle, Collection<? extends DirectSlice> c);
  private native boolean removeAll0(long handle, Collection<?> c);
  private native boolean retainAll0(long handle, Collection<?> c);
  private native void clear0(long handle);
  private native boolean contains0(long handle, Object o);
  private native int size0(long handle);
  private native boolean isEmpty0(long handle);
  
  private native Object[] toArray0(long handle);
  private native Object[] toArray1(long handle, Object[] a);
  
  private native String toString0(long handle, boolean hex);

  private native void disposeInternal(long handle);

  
  private class Iter implements Iterator<DirectSlice> {
    
    public Iter() {
      assert (isInitialized());
      this.idx_ = -1;
      this.justRemovedElem_ = false;
    }
    
    public boolean hasNext() {
      assert (isInitialized());
      return itrhasNext0(nativeHandle_, idx_ + 1);
    }
    
    public DirectSlice next() {
      assert (isInitialized());
      justRemovedElem_ = false;
      return itrNext0(nativeHandle_, ++idx_);
    }
    
    public void remove() {
      assert (isInitialized());
      
      if (justRemovedElem_) { // Client should have called next() first
        throw new IllegalStateException(
            "Cannot invoke Iterator.remove() consecutively!");
      }
      
      // We need to decrement idx_ because in java one has to call
      // next() after calling remove() to obtain the next elem.
      // Note that next() increments idx_. To avoid skipping
      // the element that is right after the one we just removed,
      // we need to decrement idx_ here.
      itrRemove0(nativeHandle_, idx_--);
      justRemovedElem_ = true;
    }
    
    private int idx_;
    private boolean justRemovedElem_;
    
    private native boolean itrhasNext0(long handle, int idx);
    private native DirectSlice itrNext0(long handle, int idx);
    private native void itrRemove0(long handle, int idx);
  }
}
