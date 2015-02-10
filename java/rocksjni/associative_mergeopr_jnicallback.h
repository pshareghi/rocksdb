// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// This file implements the callback "bridges" between Java and C++ for
// rocksdb::AssociativeMergeOperator and
// rocksdb::DirectAssociativeMergeOperator.

#ifndef JAVA_ROCKSJNI_ASSOCIATIVE_MERGEOPR_JNICALLBACK_H_
#define JAVA_ROCKSJNI_ASSOCIATIVE_MERGEOPR_JNICALLBACK_H_

#include <jni.h>

#include <string>

#include "port/port.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"
#include "rocksjni/mergeopr_jnicallback.h"

namespace rocksdb {

/*struct AssociativeMergeOprJniCallbackOptions {
  // Use adaptive mutex, which spins in the user space before resorting
  // to kernel. This could reduce context switch when the mutex is not
  // heavily contended. However, if the mutex is hot, we could end up
  // wasting spin time.
  // Default: false
  bool use_adaptive_mutex;

  AssociativeMergeOprJniCallbackOptions() :
      use_adaptive_mutex(false) {
  }
};*/

/**
 * This class acts as a bridge between C++
 * and Java. The methods in this class will be
 * called back from the RocksDB storage engine (C++)
 * we then callback to the appropriate Java method
 * this enables AssociativeMerge Operators to be
 * implemented in Java.
 *
 * The design of this Associative MergeOperator caches the Java Slice
 * objects that are used in the merge method. Instead of
 * creating new objects for each callback merge, by reuse via
 * setHandle we are a lot faster; Unfortunately this means that we
 * have to introduce independent locking in this method via the mutex
 * mtx_merge.
 */
class BaseAssociativeMergeOprJniCallback : public AssociativeMergeOperator {
 public:
  BaseAssociativeMergeOprJniCallback(
      JNIEnv* env, jobject jAssociativeMergeOpr,
      const MergeOprJniCallbackOptions* mopt);
  virtual ~BaseAssociativeMergeOprJniCallback();
  virtual const char* Name() const;
  virtual bool Merge(const Slice& key, const Slice* existing_value,
                     const Slice& value, std::string* new_value,
                     Logger* logger) const;

 private:
  // used for synchronization in Merge method
  port::Mutex* mtx_merge;

  JavaVM* m_jvm;
  jobject m_jAssociativeMergeOpr;
  std::string m_name;
  jmethodID m_jMergeMethodId;

 protected:
  JNIEnv* getJniEnv() const;
  jobject m_jKeySlice;
  jobject m_jExistingValueSlice;
  jobject m_jValueSlice;
};

class AssociativeMergeOprJniCallback :
    public BaseAssociativeMergeOprJniCallback {
 public:
  AssociativeMergeOprJniCallback(
      JNIEnv* env, jobject jMergeOpr,
      const MergeOprJniCallbackOptions* mopt);
  ~AssociativeMergeOprJniCallback();
};

class DirectAssociativeMergeOprJniCallback :
    public BaseAssociativeMergeOprJniCallback {
 public:
  DirectAssociativeMergeOprJniCallback(
      JNIEnv* env, jobject jDirectAssociativeMergeOpr,
      const MergeOprJniCallbackOptions* mopt);
  ~DirectAssociativeMergeOprJniCallback();
};
}  // namespace rocksdb

#endif  // JAVA_ROCKSJNI_ASSOCIATIVE_MERGEOPR_JNICALLBACK_H_
