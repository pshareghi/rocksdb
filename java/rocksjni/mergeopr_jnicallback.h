// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// This file implements the callback "bridges" between Java and C++ for
// rocksdb::MergeOperator and rocksdb::DirectMergeOperator.

#ifndef JAVA_ROCKSJNI_MERGEOPR_JNICALLBACK_H_
#define JAVA_ROCKSJNI_MERGEOPR_JNICALLBACK_H_

#include <jni.h>

#include <deque>
#include <string>

#include "port/port.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/slice.h"

namespace rocksdb {

struct MergeOprJniCallbackOptions {
  // Use adaptive mutex, which spins in the user space before resorting
  // to kernel. This could reduce context switch when the mutex is not
  // heavily contended. However, if the mutex is hot, we could end up
  // wasting spin time.
  // Default: false
  bool use_adaptive_mutex;

  MergeOprJniCallbackOptions() :
      use_adaptive_mutex(false) {
  }
};

/**
 * This class acts as a bridge between C++
 * and Java. The methods in this class will be
 * called back from the RocksDB storage engine (C++)
 * we then callback to the appropriate Java method
 * this enables Merge Operators to be implemented in Java.
 *
 * The design of this MergeOperator caches the Java Slice
 * objects that are used in the merge methods. Instead of
 * creating new objects for each callback  of those
 * functions, by reuse via setHandle we are a lot
 * faster; Unfortunately this means that we have to
 * introduce independent locking in regions of each of those methods
 * via the mutexs mtx_fullMerge, mtx_partialMerge,
 * and mtx_partialMergeMulti.
 */
class BaseMergeOprJniCallback : public MergeOperator {
 public:
  BaseMergeOprJniCallback(JNIEnv* env, jobject jMergeOpr,
                          const MergeOprJniCallbackOptions* mopt);
  virtual ~BaseMergeOprJniCallback();
  virtual const char* Name() const;
  virtual bool FullMerge(const Slice& key, const Slice* existing_value,
                         const std::deque<std::string>& operand_list,
                         std::string* new_value, Logger* logger) const;
  virtual bool PartialMerge(const Slice& key, const Slice& left_operand,
                            const Slice& right_operand, std::string* new_value,
                            Logger* logger) const;
  virtual bool PartialMergeMulti(const Slice& key,
                                 const std::deque<Slice>& operand_list,
                                 std::string* new_value, Logger* logger) const;

 private:
  // used for synchronization in FullMerge method
  port::Mutex* mtx_fullMerge;
  // used for synchronization in PartialMerge method
  port::Mutex* mtx_partialMerge;
  // used for synchronization in PartialMergeMulti method
  port::Mutex* mtx_partialMergeMulti;

  JavaVM* m_jvm;
  jobject m_jMergeOpr;
  std::string m_name;
  jmethodID m_jFullMergeMethodId;
  jmethodID m_jPartialMergeMethodId;
  jmethodID m_jPartialMergeMultiMethodId;

 protected:
  JNIEnv* getJniEnv() const;
  jobject m_jSliceA;
  jobject m_jSliceB;
};

class MergeOprJniCallback : public BaseMergeOprJniCallback {
 public:
  MergeOprJniCallback(JNIEnv* env, jobject jMergeOpr,
                      const MergeOprJniCallbackOptions* mopt);
  ~MergeOprJniCallback();
};

class DirectMergeOprJniCallback : public BaseComparatorJniCallback {
 public:
  DirectMergeOprJniCallback(JNIEnv* env, jobject jDirectMergeOpr,
                            const MergeOprJniCallbackOptions* mopt);
  ~DirectMergeOprJniCallback();
};
}  // namespace rocksdb

#endif  // JAVA_ROCKSJNI_MERGEOPR_JNICALLBACK_H_
