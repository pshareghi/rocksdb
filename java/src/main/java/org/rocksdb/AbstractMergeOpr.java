// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

import java.util.Deque;

/**
 * Essentially, a MergeOperator specifies the SEMANTICS
 *  of a merge, which only client knows.
 *
 * This class is package private, implementers
 * should extend either of the public abstract classes:
 *   @see org.rocksdb.MergeOpr
 *   @see org.rocksdb.DirectMergeOpr
 */
public abstract class AbstractMergeOpr<T extends AbstractSlice>
    extends RocksObject {

  /**
   * The name of the merge operator.
   *
   * Names starting with "rocksdb." are reserved and should not be used.
   *
   * @return The name of this merge operator implementation
   */
  public abstract String name();

  /**
   * NOT implemented at the moment.
   * 
   * TODO(pshareghi): Fix type inconsistencies. In C++, opernad_list is
   * of type string, the dequeue is a reference, and new value is a
   *  string*. There is also a Logger* as a last parameter.
   */
  public boolean fullMerge(T key,
          T existing_value,
          Deque<T> operand_list,
          T new_value) {
	  return false;
  }

  /**
   * Not implemented at the moment.
   * 
   * TODO(pshareghi): Fix type inconsistencies. In C++, new value is a
   *  string*. There is also a Logger* as a last parameter.
   */
  public boolean partialMerge(T key, T left_operand,
          T right_operand, T new_value)  {
      return false;
  }

  /**
   * Not implemented at the moment.
   * 
   * TODO(pshareghi): Fix type inconsistencies. In C++, opernad_list is
   * of type string, the dequeue is a reference, and new value is a
   *  string*. There is also a Logger* as a last parameter.
   */
  public boolean partialMergeMulti(T key,
          Deque<T> operand_list,
          T new_value) {
      return false;
  }

  /**
   * Deletes underlying C++ comparator pointer.
   *
   * Note that this function should be called only after all
   * RocksDB instances referencing the merge operator are closed.
   * Otherwise an undefined behavior will occur.
   */
  @Override protected void disposeInternal() {
    assert(isInitialized());
    disposeInternal(nativeHandle_);
  }

  private native void disposeInternal(long handle);
}
