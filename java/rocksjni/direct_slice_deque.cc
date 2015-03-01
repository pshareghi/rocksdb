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

#include "include/org_rocksdb_DirectSliceDeque.h"
#include "include/org_rocksdb_DirectSliceDeque_Iter.h"
#include "portal.h"

/*
 * Class:     org_rocksdb_DirectSliceDeque
 * Method:    addFirst0
 * Signature: (JLorg/rocksdb/DirectSlice;)V
 */
void JNICALL Java_org_rocksdb_DirectSliceDeque_addFirst0(JNIEnv* env, jobject jobj,
                                                   jlong handle,
                                                   jobject jelem) {
  // Get c++ Slice ptr
  const auto slice = rocksdb::AbstractSliceJni::getHandle(env, jelem);

  // Add to the front of deque
  const auto deque = reinterpret_cast<std::deque<rocksdb::Slice> *>(handle);
  deque->push_front(*slice);
}

/*
 * Class:     org_rocksdb_DirectSliceDeque
 * Method:    addLast0
 * Signature: (JLorg/rocksdb/DirectSlice;)V
 */
void JNICALL Java_org_rocksdb_DirectSliceDeque_addLast0(JNIEnv* env, jobject jobj,
                                                  jlong handle, jobject jelem) {
  // Get c++ Slice ptr
  const auto slice = rocksdb::AbstractSliceJni::getHandle(env, jelem);

  // Add to the end of deque
  const auto deque = reinterpret_cast<std::deque<rocksdb::Slice> *>(handle);
  deque->push_back(*slice);
}

/*
 * Class:     org_rocksdb_DirectSliceDeque
 * Method:    removeFirst0
 * Signature: (J)Lorg/rocksdb/DirectSlice;
 */
jobject JNICALL Java_org_rocksdb_DirectSliceDeque_removeFirst0(JNIEnv* env,
                                                         jobject jobj,
                                                         jlong handle) {
  const auto deque = reinterpret_cast<std::deque<rocksdb::Slice> *>(handle);

  if (deque->empty()) {
    rocksdb::ExceptionJni::ThrowNew(
        env, "java/util/NoSuchElementException",
        "Cannot invoke removeFirst on an empty deque!");
    return NULL;
  }

  rocksdb::Slice& slice = deque->front();

  // Create a java DirectSlice object to hold its c++ counterpart
  jobject jslice = rocksdb::DirectSliceJni::construct0(env);
  rocksdb::AbstractSliceJni::setHandle(env, jslice, &slice);

  deque->pop_front();

  return jslice;
}

/*
 * Class:     org_rocksdb_DirectSliceDeque
 * Method:    removeLast0
 * Signature: (J)Lorg/rocksdb/DirectSlice;
 */
jobject JNICALL Java_org_rocksdb_DirectSliceDeque_removeLast0(JNIEnv* env,
                                                        jobject jobj,
                                                        jlong handle) {
  const auto deque = reinterpret_cast<std::deque<rocksdb::Slice> *>(handle);

  if (deque->empty()) {
    rocksdb::ExceptionJni::ThrowNew(
        env, "java/util/NoSuchElementException",
        "Cannot invoke removeLast on an empty deque!");
    return NULL;
  }

  rocksdb::Slice& slice = deque->back();

  // Create a java DirectSlice object to hold its c++ counterpart
  jobject jslice = rocksdb::DirectSliceJni::construct0(env);
  rocksdb::AbstractSliceJni::setHandle(env, jslice, &slice);

  deque->pop_back();

  return jslice;
}

/*
 * Class:     org_rocksdb_DirectSliceDeque
 * Method:    pollFirst0
 * Signature: (J)Lorg/rocksdb/DirectSlice;
 */
jobject JNICALL Java_org_rocksdb_DirectSliceDeque_pollFirst0(JNIEnv* env,
                                                       jobject jobj,
                                                       jlong handle) {
  const auto deque = reinterpret_cast<std::deque<rocksdb::Slice> *>(handle);

  if (deque->empty()) {
    return NULL;
  }

  rocksdb::Slice& slice = deque->front();

  // Create a java DirectSlice object to hold its c++ counterpart
  jobject jslice = rocksdb::DirectSliceJni::construct0(env);
  rocksdb::AbstractSliceJni::setHandle(env, jslice, &slice);

  return jslice;
}

/*
 * Class:     org_rocksdb_DirectSliceDeque
 * Method:    pollLast0
 * Signature: (J)Lorg/rocksdb/DirectSlice;
 */
JNIEXPORT jobject JNICALL Java_org_rocksdb_DirectSliceDeque_pollLast0(JNIEnv* env, jobject jobj,
                                                      jlong handle) {
  const auto deque = reinterpret_cast<std::deque<rocksdb::Slice> *>(handle);

  if (deque->empty()) {
    return NULL;
  }

  rocksdb::Slice& slice = deque->back();

  // Create a java DirectSlice object to hold its c++ counterpart
  jobject jslice = rocksdb::DirectSliceJni::construct0(env);
  rocksdb::AbstractSliceJni::setHandle(env, jslice, &slice);

  return jslice;
}

/*
 * Class:     org_rocksdb_DirectSliceDeque
 * Method:    getFirst0
 * Signature: (J)Lorg/rocksdb/DirectSlice;
 */
jobject JNICALL Java_org_rocksdb_DirectSliceDeque_getFirst0(JNIEnv* env, jobject jobj,
                                                      jlong handle) {
  const auto deque = reinterpret_cast<std::deque<rocksdb::Slice> *>(handle);

  if (deque->empty()) {
    rocksdb::ExceptionJni::ThrowNew(
        env, "java/util/NoSuchElementException",
        "Cannot invoke getFirst on an empty deque!");
    return NULL;
  }

  rocksdb::Slice& slice = deque->front();

  // Create a java DirectSlice object to hold its c++ counterpart
  jobject jslice = rocksdb::DirectSliceJni::construct0(env);
  rocksdb::AbstractSliceJni::setHandle(env, jslice, &slice);

  return jslice;
}

/*
 * Class:     org_rocksdb_DirectSliceDeque
 * Method:    getLast0
 * Signature: (J)Lorg/rocksdb/DirectSlice;
 */
JNIEXPORT jobject JNICALL Java_org_rocksdb_DirectSliceDeque_getLast0(JNIEnv* env, jobject jobj,
                                                     jlong handle) {
  const auto deque = reinterpret_cast<std::deque<rocksdb::Slice> *>(handle);

  if (deque->empty()) {
    rocksdb::ExceptionJni::ThrowNew(env, "java/util/NoSuchElementException",
                                    "Cannot invoke getLast on an empty deque!");
    return NULL;
  }

  rocksdb::Slice& slice = deque->back();

  // Create a java DirectSlice object to hold its c++ counterpart
  jobject jslice = rocksdb::DirectSliceJni::construct0(env);
  rocksdb::AbstractSliceJni::setHandle(env, jslice, &slice);

  return jslice;
}

/*
 * Class:     org_rocksdb_DirectSliceDeque
 * Method:    peekFirst0
 * Signature: (J)Lorg/rocksdb/DirectSlice;
 */
jobject JNICALL Java_org_rocksdb_DirectSliceDeque_peekFirst0(JNIEnv* env,
                                                       jobject jobj,
                                                       jlong handle) {
  const auto deque = reinterpret_cast<std::deque<rocksdb::Slice> *>(handle);

  if (deque->empty()) {
    return NULL;
  }

  rocksdb::Slice& slice = deque->front();

  // Create a java DirectSlice object to hold its c++ counterpart
  jobject jslice = rocksdb::DirectSliceJni::construct0(env);
  rocksdb::AbstractSliceJni::setHandle(env, jslice, &slice);

  return jslice;
}

/*
 * Class:     org_rocksdb_DirectSliceDeque
 * Method:    peekLast0
 * Signature: (J)Lorg/rocksdb/DirectSlice;
 */
jobject JNICALL Java_org_rocksdb_DirectSliceDeque_peekLast0(JNIEnv* env, jobject jobj,
                                                      jlong handle) {
  const auto deque = reinterpret_cast<std::deque<rocksdb::Slice> *>(handle);

  if (deque->empty()) {
    return NULL;
  }

  rocksdb::Slice& slice = deque->back();

  // Create a java DirectSlice object to hold its c++ counterpart
  jobject jslice = rocksdb::DirectSliceJni::construct0(env);
  rocksdb::AbstractSliceJni::setHandle(env, jslice, &slice);

  return jslice;
}

/*
 * Class:     org_rocksdb_DirectSliceDeque
 * Method:    removeFirstOccurrence0
 * Signature: (JLjava/lang/Object;)Z
 */
jboolean JNICALL Java_org_rocksdb_DirectSliceDeque_removeFirstOccurrence0(
    JNIEnv* env, jobject jobj, jlong handle, jobject elem) {
  rocksdb::ExceptionJni::ThrowNew(env,
                                  "java/lang/UnsupportedOperationException",
                                  "");
  return false;
}

/*
 * Class:     org_rocksdb_DirectSliceDeque
 * Method:    removeLastOccurrence0
 * Signature: (JLjava/lang/Object;)Z
 */
jboolean JNICALL Java_org_rocksdb_DirectSliceDeque_removeLastOccurrence0(
    JNIEnv* env, jobject jobj, jlong handle, jobject elem) {
  rocksdb::ExceptionJni::ThrowNew(env,
                                  "java/lang/UnsupportedOperationException",
                                  "");
  return false;
}

/*
 * Class:     org_rocksdb_DirectSliceDeque
 * Method:    remove0
 * Signature: (JLjava/lang/Object;)Z
 */
jboolean JNICALL Java_org_rocksdb_DirectSliceDeque_remove0(JNIEnv* env, jobject jobj,
                                                     jlong handle,
                                                     jobject elem) {
  rocksdb::ExceptionJni::ThrowNew(env,
                                  "java/lang/UnsupportedOperationException",
                                  "");
  return false;
}

/*
 * Class:     org_rocksdb_DirectSliceDeque
 * Method:    containsAll0
 * Signature: (JLjava/util/Collection;)Z
 */
jboolean JNICALL Java_org_rocksdb_DirectSliceDeque_containsAll0(JNIEnv* env,
                                                          jobject jobj,
                                                          jlong handle,
                                                          jobject elem) {
  rocksdb::ExceptionJni::ThrowNew(env,
                                  "java/lang/UnsupportedOperationException",
                                  "");
  return false;
}

/*
 * Class:     org_rocksdb_DirectSliceDeque
 * Method:    addAll0
 * Signature: (JLjava/util/Collection;)Z
 */
jboolean JNICALL Java_org_rocksdb_DirectSliceDeque_addAll0(JNIEnv* env, jobject jobj,
                                                     jlong handle,
                                                     jobject elem) {
  rocksdb::ExceptionJni::ThrowNew(env,
                                  "java/lang/UnsupportedOperationException",
                                  "");
  return false;
}

/*
 * Class:     org_rocksdb_DirectSliceDeque
 * Method:    removeAll0
 * Signature: (JLjava/util/Collection;)Z
 */
jboolean JNICALL Java_org_rocksdb_DirectSliceDeque_removeAll0(JNIEnv* env,
                                                        jobject jobj,
                                                        jlong handle,
                                                        jobject elem) {
  rocksdb::ExceptionJni::ThrowNew(env,
                                  "java/lang/UnsupportedOperationException",
                                  "");
  return false;
}

/*
 * Class:     org_rocksdb_DirectSliceDeque
 * Method:    retainAll0
 * Signature: (JLjava/util/Collection;)Z
 */
jboolean JNICALL Java_org_rocksdb_DirectSliceDeque_retainAll0(JNIEnv* env,
                                                        jobject jobj,
                                                        jlong handle,
                                                        jobject elem) {
  rocksdb::ExceptionJni::ThrowNew(env,
                                  "java/lang/UnsupportedOperationException",
                                  "");
  return false;
}

/*
 * Class:     org_rocksdb_DirectSliceDeque
 * Method:    clear0
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_rocksdb_DirectSliceDeque_clear0(JNIEnv* env, jobject jobj,
                                                jlong handle) {
  const auto deque = reinterpret_cast<std::deque<rocksdb::Slice> *>(handle);

  deque->clear();
}

/*
 * Class:     org_rocksdb_DirectSliceDeque
 * Method:    contains0
 * Signature: (JLjava/lang/Object;)Z
 */
jboolean JNICALL Java_org_rocksdb_DirectSliceDeque_contains0(JNIEnv* env,
                                                       jobject jobj,
                                                       jlong handle,
                                                       jobject elem) {
  rocksdb::ExceptionJni::ThrowNew(env,
                                  "java/lang/UnsupportedOperationException",
                                  "");
  return false;
}

/*
 * Class:     org_rocksdb_DirectSliceDeque
 * Method:    size0
 * Signature: (J)I
 */
jint JNICALL Java_org_rocksdb_DirectSliceDeque_size0(JNIEnv* env, jobject jobj,
                                               jlong handle) {
  const auto deque = reinterpret_cast<std::deque<rocksdb::Slice> *>(handle);

  return deque->size();
}

/*
 * Class:     org_rocksdb_DirectSliceDeque
 * Method:    isEmpty0
 * Signature: (J)Z
 */
jboolean JNICALL Java_org_rocksdb_DirectSliceDeque_isEmpty0(JNIEnv* env, jobject jobj,
                                                      jlong handle) {
  const auto deque = reinterpret_cast<std::deque<rocksdb::Slice> *>(handle);

  return deque->empty();
}

/*
 * Class:     org_rocksdb_DirectSliceDeque
 * Method:    toArray0
 * Signature: (J)[Ljava/lang/Object;
 */
jobjectArray JNICALL Java_org_rocksdb_DirectSliceDeque_toArray0(JNIEnv* env,
                                                          jobject jobj,
                                                          jlong handle) {
  rocksdb::ExceptionJni::ThrowNew(env,
                                  "java/lang/UnsupportedOperationException",
                                  "");
  return NULL;
}

/*
 * Class:     org_rocksdb_DirectSliceDeque
 * Method:    toArray1
 * Signature: (J[Ljava/lang/Object;)[Ljava/lang/Object;
 */
jobjectArray JNICALL Java_org_rocksdb_DirectSliceDeque_toArray1(JNIEnv* env,
                                                          jobject jobj,
                                                          jlong handle,
                                                          jobjectArray array) {
  rocksdb::ExceptionJni::ThrowNew(env,
                                  "java/lang/UnsupportedOperationException",
                                  "");
  return NULL;
}

/*
 * Class:     org_rocksdb_DirectSliceDeque
 * Method:    toString0
 * Signature: (JZ)Ljava/lang/String;
 */
jstring JNICALL Java_org_rocksdb_DirectSliceDeque_toString0(JNIEnv* env, jobject jobj,
                                                      jlong handle,
                                                      jboolean hex) {
  rocksdb::ExceptionJni::ThrowNew(env,
                                  "java/lang/UnsupportedOperationException",
                                  "");
  return NULL;
}

/*
 * Class:     org_rocksdb_DirectSliceDeque
 * Method:    disposeInternal
 * Signature: (J)V
 */
void JNICALL Java_org_rocksdb_DirectSliceDeque_disposeInternal(
    JNIEnv* env, jobject jobj, jlong handle) {
  delete reinterpret_cast<std::deque<rocksdb::Slice> *>(handle);
}

////////////////////////////// Iterator implementation
/*
 * Class:     org_rocksdb_DirectSliceDeque_Iter
 * Method:    itrhasNext0
 * Signature: (JI)Z
 */
jboolean JNICALL Java_org_rocksdb_DirectSliceDeque_00024Iter_itrhasNext0(JNIEnv* env,
                                                                   jobject jobj,
                                                                   jlong handle,
                                                                   jint idx) {
  const auto deque = reinterpret_cast<std::deque<rocksdb::Slice> *>(handle);
  auto iter = deque->begin() + idx;
  return (iter >= deque->end());
}

/*
 * Class:     org_rocksdb_DirectSliceDeque_Iter
 * Method:    itrNext0
 * Signature: (JI)Lorg/rocksdb/DirectSlice;
 */
jobject JNICALL Java_org_rocksdb_DirectSliceDeque_00024Iter_itrNext0(JNIEnv* env,
                                                               jobject jobj,
                                                               jlong handle,
                                                               jint idx) {
  const auto deque = reinterpret_cast<std::deque<rocksdb::Slice> *>(handle);
  auto iter = deque->begin() + idx;
  if (iter >= deque->end()) {
    rocksdb::ExceptionJni::ThrowNew(
        env, "java/util/NoSuchElementException",
        "Cannot invoke iterator.next() past the last element!");
    return NULL;
  }

  // Create a java DirectSlice object to hold its c++ counterpart
  jobject jslice = rocksdb::DirectSliceJni::construct0(env);
  rocksdb::AbstractSliceJni::setHandle(env, jslice, &(*iter));

  return jslice;
}

/*
 * Class:     org_rocksdb_DirectSliceDeque_Iter
 * Method:    itrRemove0
 * Signature: (JI)V
 */
void JNICALL Java_org_rocksdb_DirectSliceDeque_00024Iter_itrRemove0(JNIEnv* env,
                                                              jobject jobj,
                                                              jlong handle,
                                                              jint idx) {
  const auto deque = reinterpret_cast<std::deque<rocksdb::Slice> *>(handle);
  auto iter = deque->begin() + idx;
  deque->erase(iter);
}

