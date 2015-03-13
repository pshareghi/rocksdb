// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
/**
 * Copyright (C) 2011 the original author or authors.
 * See the notice.md file distributed with this work for additional
 * information regarding copyright ownership.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.rocksdb;

import org.rocksdb.AssociativeMergeOpr;

public class BytesXOROpr extends AssociativeMergeOpr {
  
  public BytesXOROpr(final MergeOprOptions mopt) throws RocksDBException {
    super(mopt);
  }
  
  @Override
  public byte[] merge(Slice key,
      Slice existingValue,
      Slice value) {
    if (!existingValue.isInitialized()) {
      return value.data();
    }
    return xorBytes(existingValue.data(), value.data());
  }
  
  public static byte[] xorBytes(byte[] existingValue, byte[] value) {
    byte[] mergeResult = new byte[value.length];
    
    for (int i = 0; i < value.length; i++) {
      mergeResult[i] = (byte) (existingValue[i] ^ value[i]); 
    }
    
    return mergeResult;
  }
}
