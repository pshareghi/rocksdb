// Copyright (c) 2013, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include "bytesxor.h"

namespace rocksdb {

std::shared_ptr<MergeOperator> MergeOperators::CreateBytesXOROperator() {
  return std::make_shared<BytesXOROperator>();
}

bool BytesXOROperator::Merge(const Slice& key,
                            const Slice* existing_value,
                            const Slice& value,
                            std::string* new_value,
                            Logger* logger) const {

  if (!existing_value){
   new_value->clear();
   new_value->assign(value.data(), value.size());
   return true;
  }

  if (existing_value->size() != value.size()) {
   return false;
  }

  XorBytes(existing_value->data(), value.data(), value.size(), new_value);

  return true;
}

void BytesXOROperator::XorBytes(const char* array1, const char* array2, int len,
                        std::string* new_value) {
  assert(array1);
  assert(array2);
  assert(new_value);
  new_value->clear();

  new_value->reserve(len);

  for (int i = 0; i < len; i++) {
    new_value->push_back(array1[i] ^ array2[i]);
  }
}

}
