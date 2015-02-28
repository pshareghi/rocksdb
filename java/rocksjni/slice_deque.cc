// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// This file implements the "bridge" between Java and C++ for
// std::deque<Slice>.

#include <jni.h>

#include <deque>
#include <memory>
#include <string>

#include "include/org_rocksdb_ByteArrayDeque.h"
#include "include/org_rocksdb_ByteArrayDeque_Iter.h"
#include "portal.h"

/*
 * Class:     org_rocksdb_SliceDeque
 * Method:    addFirst0
 * Signature: (JLorg/rocksdb/Slice;)V
 */
void JNICALL Java_org_rocksdb_SliceDeque_addFirst0(JNIEnv* env,
                                                       jobject jobj,
                                                       jlong handle,
                                                       jobject jelem) {
  // Get c++ Slice ptr
  const auto slice = rocksdb::AbstractSliceJni::getHandle(env, jelem);

  // Add to the front of deque
  const auto deque = reinterpret_cast<std::deque<rocksdb::Slice> *>(handle);
  deque->push_front(*slice);
}

/*
 * Class:     org_rocksdb_SliceDeque
 * Method:    addLast0
 * Signature: (JLorg/rocksdb/Slice;)V
 */
void JNICALL Java_org_rocksdb_SliceDeque_addLast0(JNIEnv* env, jobject jobj,
                                                      jlong handle,
                                                      jobject jelem) {
  // Get c++ Slice ptr
  const auto slice = rocksdb::AbstractSliceJni::getHandle(env, jelem);

  // Add to the end of deque
  const auto deque = reinterpret_cast<std::deque<rocksdb::Slice> *>(handle);
  deque->push_back(*slice);
}

/*
 * Class:     org_rocksdb_SliceDeque
 * Method:    removeFirst0
 * Signature: (J)Lorg/rocksdb/Slice;
 */
jobject JNICALL Java_org_rocksdb_SliceDeque_removeFirst0(JNIEnv* env,
                                                         jobject jobj,
                                                         jlong handle) {
  const auto deque = reinterpret_cast<std::deque<rocksdb::Slice> *>(handle);

  if (deque->empty()) {
    rocksdb::ExceptionJni::ThrowNew(
        env, "java/util/NoSuchElementException",
        "Cannot invoke removeFirst on an empty deque!");
    return NULL;
  }

  const rocksdb::Slice& slice = deque->front();

  // Create a java Slice object to hold its c++ counterpart
  jobject jslice = rocksdb::SliceJni::construct0(env);
  rocksdb::AbstractSliceJni::setHandle(env, jslice, &slice);

  deque->pop_front();

  return jslice;
}



