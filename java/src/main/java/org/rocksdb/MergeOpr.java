// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

/**
 * Base class for merge operators which will receive byte[] based access via
 * org.rocksdb.Slice in their merge method implementation.
 *
 * byte[] based slices perform better when small keys are involved. When using
 * larger keys consider using @see org.rocksdb.DirectMergeOpr
 */
public abstract class MergeOpr extends AbstractMergeOpr<Slice> {
  public MergeOpr(final MergeOprOptions mopt) {
    super();
    createNewMergeOpr0(mopt.nativeHandle_);
  }

  private native void createNewMergeOpr0(final long mergeOprOptionsHandle);
}
