/*
 * bytesxor.h
 *
 *  Created on: Mar 13, 2015
 *      Author: pshareghi
 */

#ifndef UTILITIES_MERGE_OPERATORS_BYTESXOR_H_
#define UTILITIES_MERGE_OPERATORS_BYTESXOR_H_

#include <algorithm>
#include <memory>
#include "rocksdb/env.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"
#include "util/coding.h"
#include "utilities/merge_operators.h"


namespace rocksdb {

// A 'model' merge operator that XORs two (same sized) array of bytes.
// Implemented as an AssociativeMergeOperator for simplicity and example.
class BytesXOROperator : public AssociativeMergeOperator {
 public:
  virtual bool Merge(const Slice& key,
                     const Slice* existing_value,
                     const Slice& value,
                     std::string* new_value,
                     Logger* logger) const override ;

  // XORs the two array of bytes one byte at a time and stores the result
  // in new_value. len is the number of xored bytes, and the length of new_value
  static void XorBytes(const char* array1, const char* array2, int len,
                        std::string* new_value);

  virtual const char* Name() const override {
    return "BytesXOR";
  }
};

}

#endif /* UTILITIES_MERGE_OPERATORS_BYTESXOR_H_ */
