// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

import java.util.Deque;

/**
 * Base class for associativne merge operators which will receive ByteBuffer
 * based access via org.rocksdb.DirectSlice in their merge method
 * implementation.
 *
 * ByteBuffer based slices perform better when large keys are involved. When
 * using smaller keys consider using @see org.rocksdb.AssociativeMergeOpr
 */
public abstract class DirectAssociativeMergeOpr extends DirectMergeOpr {
  public DirectAssociativeMergeOpr(final MergeOprOptions mopt) {
    super(mopt);
    createNewDirectAssociativeMergeOpr0(mopt.nativeHandle_);
  }

  /**
   * The native handler does not do a JNI callback for this method It directly
   * calls the C++ AssociativeMerge implementation. The method is made final so
   * that no subclass can implement it differently.
   *
   * TODO(pshareghi): Fix type inconsistencies. In C++, opernad_list is of type
   * string, the dequeue is a reference, and new value is a string*. There is
   * also a Logger* as a last parameter.
   */
  public final boolean fullMerge(DirectSlice key, DirectSlice existingValue,
      Deque<DirectSlice> operandList, DirectSlice newValue) {
    return true;
  }

  /**
   * The native handler does not do a JNI callback for this method It directly
   * calls the C++ AssociativeMerge implementation. The method is made final so
   * that no subclass can implement it differently.
   *
   * TODO(pshareghi): Fix type inconsistencies. In C++, new value is a string*.
   * There is also a Logger* as a last parameter.
   */
  public final boolean partialMerge(DirectSlice key, DirectSlice leftOperand,
      DirectSlice rightOperand, DirectSlice newValue) {
    return true;
  }

  /**
   * The native handler does not do a JNI callback for this method It directly
   * calls the C++ AssociativeMerge implementation. The method is made final so
   * that no subclass can implement it differently.
   *
   * TODO(pshareghi): Fix type inconsistencies. In C++, opernad_list is of type
   * string, the dequeue is a reference, and new value is a string*. There is
   * also a Logger* as a last parameter.
   */
  public final boolean partialMergeMulti(DirectSlice key,
      Deque<DirectSlice> operandList, DirectSlice newValue) {
    return true;
  }

  public abstract boolean Merge(Slice key,
      Slice existingValue,
      Slice value,
      Slice newValue);
  
  /**
   * Deletes underlying C++ comparator pointer.
   *
   * Note that this function should be called only after all RocksDB instances
   * referencing the merge operator are closed. Otherwise an undefined behavior
   * will occur.
   */
  @Override
  protected void disposeInternal() {
    assert (isInitialized());
    disposeInternal(nativeHandle_);
  }

  private native void disposeInternal(long handle);
  
  private native void createNewDirectAssociativeMergeOpr0(
      final long mergeOprOptionsHandle);
}
