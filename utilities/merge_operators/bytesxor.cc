// Copyright (c) 2013, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.

#include <algorithm>
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

  XorBytes(existing_value->data(), existing_value->size(),
		  value.data(), value.size(), new_value);

  return true;
}

void BytesXOROperator::XorBytes(const char* array1, int array1_len,
                        const char* array2, int array2_len,
                        std::string* new_value) {
  assert(array1);
  assert(array2);
  assert(new_value);
  new_value->clear();

  int min_len = std::min(array1_len, array2_len);
  int max_len = std::max(array1_len, array2_len);

  new_value->resize(max_len);

  for (int i = 0; i < min_len; i++) {
    (*new_value)[i] = array1[i] ^ array2[i];
  }

  if (array1_len > array2_len) {
	  for (int i = array2_len; i < array1_len; i++) {
		  (*new_value)[i] = array1[i];
      }
  } else if (array2_len > array1_len) {
	  for (int i = array1_len; i < array2_len; i++) {
		  (*new_value)[i] = array2[i];
	  }
  }
}

}
