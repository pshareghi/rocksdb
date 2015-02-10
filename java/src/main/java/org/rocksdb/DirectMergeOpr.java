// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

/**
 * Base class for merge operators which will receive ByteBuffer based access via
 * org.rocksdb.DirectSlice in their merge method implementation.
 *
 * ByteBuffer based slices perform better when large keys are involved. When
 * using smaller keys consider using @see org.rocksdb.MergeOp
 */
public abstract class DirectMergeOpr extends AbstractMergeOpr<DirectSlice> {
  public DirectMergeOpr(final MergeOprOptions mopt) {
    super();
    createNewDirectMergeOpr0(mopt.nativeHandle_);
  }

  private native void createNewDirectMergeOpr0(final long mergeOprOptionsHandle);
}
