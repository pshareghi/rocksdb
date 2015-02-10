// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

package org.rocksdb;

import java.util.concurrent.atomic.AtomicLong;

//A 'model' merge operator with long addition semantics
//Implemented as an AssociativeMergeOperator for simplicity and example.
public class LongAddMergeOpr extends AssociativeMergeOpr {
  public LongAddMergeOpr(final MergeOprOptions mopt) throws RocksDBException {
    super(mopt);
  }
  
  @Override
  public byte[] merge(Slice key,
      Slice existingValue,
      Slice value) {
    byte[] existingValueBytes = null;
    if (existingValue.isInitialized()) {
      existingValueBytes = existingValue.data();
    }
    return sum(existingValueBytes, value.data());
  }
  
  public static byte[] sum(byte[] existingValue, byte[] value) {
    long leftOpr = 0;
    if (existingValue != null) {
      leftOpr = decodeLong(existingValue);
    }
    long rightOpr = decodeLong(value);
    
    return encodeLong(leftOpr + rightOpr);
  }
  
  public static long decodeLong(byte[] array) {
    return
        ((long)(array[0] & 0xff) << 56) |
        ((long)(array[1] & 0xff) << 48) |
        ((long)(array[2] & 0xff) << 40) |
        ((long)(array[3] & 0xff) << 32) |
        ((long)(array[4] & 0xff) << 24) |
        ((long)(array[5] & 0xff) << 16) |
        ((long)(array[6] & 0xff) << 8) |
        ((long)(array[7] & 0xff));
    }
  
  public static byte[] encodeLong(long value) {
    byte[] result = new byte[8];
    for (int i = 7; i >= 0; i--) {
      result[i] = (byte) (value & 0xffL);
      value >>= 8;
    }
    return result;
  }
}
