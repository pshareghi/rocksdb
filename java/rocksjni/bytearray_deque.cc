// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// This file implements the "bridge" between Java and C++ for
// std::.

#include <jni.h>

#include <deque>
#include <memory>
#include <string>

#include "include/org_rocksdb_ByteArrayDeque.h"
#include "include/org_rocksdb_ByteArrayDeque_Iter.h"
#include "portal.h"

/*
 * Class:     org_rocksdb_ByteArrayDeque
 * Method:    addFirst0
 * Signature: (J[B)V
 */
void JNICALL Java_org_rocksdb_ByteArrayDeque_addFirst0(JNIEnv* env,
                                                       jobject jobj,
                                                       jlong handle,
                                                       jbyteArray elem) {
  // Convert byte[] to std::string
  const auto ptr = rocksdb::JniUtil::byteArrayToStdString(env, elem);
  const auto str = std::shared_ptr<std::string>(ptr);

  // Add to the front of deque
  const auto deque = reinterpret_cast<std::deque<std::string> *>(handle);
  deque->push_front(*str);
}

/*
 * Class:     org_rocksdb_ByteArrayDeque
 * Method:    addLast0
 * Signature: (J[B)V
 */
void JNICALL Java_org_rocksdb_ByteArrayDeque_addLast0(JNIEnv* env, jobject jobj,
                                                      jlong handle,
                                                      jbyteArray elem) {
  // Convert byte[] to std::string
  const auto ptr = rocksdb::JniUtil::byteArrayToStdString(env, elem);
  const auto str = std::shared_ptr<std::string>(ptr);

  // Add to the end of deque
  const auto deque = reinterpret_cast<std::deque<std::string> *>(handle);
  deque->push_back(*str);
}

/*
 * Class:     org_rocksdb_ByteArrayDeque
 * Method:    removeFirst0
 * Signature: (J)[B
 */
jbyteArray JNICALL Java_org_rocksdb_ByteArrayDeque_removeFirst0(JNIEnv* env,
                                                                jobject jobj,
                                                                jlong handle) {
  const auto deque = reinterpret_cast<std::deque<std::string> *>(handle);

  if (deque->empty()) {
    rocksdb::ExceptionJni::ThrowNew(
        env, "java/util/NoSuchElementException",
        "Cannot invoke removeFirst on an empty deque!");
    return NULL;
  }

  const std::string& str = deque->front();
  jbyteArray elem = rocksdb::JniUtil::stdStringToByteArray(env, str);

  deque->pop_front();

  return elem;
}

/*
 * Class:     org_rocksdb_ByteArrayDeque
 * Method:    removeLast0
 * Signature: (J)[B
 */
jbyteArray JNICALL Java_org_rocksdb_ByteArrayDeque_removeLast0(JNIEnv* env,
                                                               jobject jobj,
                                                               jlong handle) {
  const auto deque = reinterpret_cast<std::deque<std::string> *>(handle);

  if (deque->empty()) {
    rocksdb::ExceptionJni::ThrowNew(
        env, "java/util/NoSuchElementException",
        "Cannot invoke removeLast on an empty deque!");
    return NULL;
  }

  std::string& str(deque->back());
  jbyteArray elem = rocksdb::JniUtil::stdStringToByteArray(env, str);

  deque->pop_back();

  return elem;
}

/*
 * Class:     org_rocksdb_ByteArrayDeque
 * Method:    pollFirst0
 * Signature: (J)[B
 */
jbyteArray JNICALL Java_org_rocksdb_ByteArrayDeque_pollFirst0(JNIEnv* env,
                                                              jobject jobj,
                                                              jlong handle) {
  const auto deque = reinterpret_cast<std::deque<std::string> *>(handle);

  if (deque->empty()) {
    return NULL;
  }

  const std::string& str = deque->front();
  jbyteArray elem = rocksdb::JniUtil::stdStringToByteArray(env, str);

  return elem;
}

/*
 * Class:     org_rocksdb_ByteArrayDeque
 * Method:    pollLast0
 * Signature: (J)[B
 */
jbyteArray JNICALL Java_org_rocksdb_ByteArrayDeque_pollLast0(JNIEnv* env,
                                                             jobject jobj,
                                                             jlong handle) {
  const auto deque = reinterpret_cast<std::deque<std::string> *>(handle);

  if (deque->empty()) {
    return NULL;
  }

  const std::string& str = deque->back();
  jbyteArray elem = rocksdb::JniUtil::stdStringToByteArray(env, str);

  return elem;
}

/*
 * Class:     org_rocksdb_ByteArrayDeque
 * Method:    getFirst0
 * Signature: (J)[B
 */
jbyteArray JNICALL Java_org_rocksdb_ByteArrayDeque_getFirst0(JNIEnv* env,
                                                             jobject jobj,
                                                             jlong handle) {
  const auto deque = reinterpret_cast<std::deque<std::string> *>(handle);

  if (deque->empty()) {
    rocksdb::ExceptionJni::ThrowNew(
        env, "java/util/NoSuchElementException",
        "Cannot invoke getFirst on an empty deque!");
    return NULL;
  }

  const std::string& str = deque->front();
  jbyteArray elem = rocksdb::JniUtil::stdStringToByteArray(env, str);

  return elem;
}

/*
 * Class:     org_rocksdb_ByteArrayDeque
 * Method:    getLast0
 * Signature: (J)[B
 */
jbyteArray JNICALL Java_org_rocksdb_ByteArrayDeque_getLast0(JNIEnv* env,
                                                            jobject jobj,
                                                            jlong handle) {
  const auto deque = reinterpret_cast<std::deque<std::string> *>(handle);

  if (deque->empty()) {
    rocksdb::ExceptionJni::ThrowNew(env, "java/util/NoSuchElementException",
                                    "Cannot invoke getLast on an empty deque!");
    return NULL;
  }

  const std::string& str = deque->back();
  jbyteArray elem = rocksdb::JniUtil::stdStringToByteArray(env, str);

  return elem;
}

/*
 * Class:     org_rocksdb_ByteArrayDeque
 * Method:    getLast0
 * Signature: (J)[B
 */
jbyteArray JNICALL Java_org_rocksdb_ByteArrayDeque_peekFirst0(JNIEnv* env,
                                                              jobject jobj,
                                                              jlong handle) {
  const auto deque = reinterpret_cast<std::deque<std::string> *>(handle);

  if (deque->empty()) {
    return NULL;
  }

  const std::string& str = deque->front();
  jbyteArray elem = rocksdb::JniUtil::stdStringToByteArray(env, str);

  return elem;
}

/*
 * Class:     org_rocksdb_ByteArrayDeque
 * Method:    getLast0
 * Signature: (J)[B
 */
jbyteArray JNICALL Java_org_rocksdb_ByteArrayDeque_peekLast0(JNIEnv* env,
                                                             jobject jobj,
                                                             jlong handle) {
  const auto deque = reinterpret_cast<std::deque<std::string> *>(handle);

  if (deque->empty()) {
    return NULL;
  }

  const std::string& str = deque->back();
  jbyteArray elem = rocksdb::JniUtil::stdStringToByteArray(env, str);

  return elem;
}

/*
 * Class:     org_rocksdb_ByteArrayDeque
 * Method:    removeFirstOccurrence0
 * Signature: (JLjava/lang/Object;)Z
 */
jboolean JNICALL Java_org_rocksdb_ByteArrayDeque_removeFirstOccurrence0(
    JNIEnv* env, jobject jobj, jlong handle, jobject elem) {
  rocksdb::ExceptionJni::ThrowNew(env,
                                  "java/lang/UnsupportedOperationException",
                                  "");
  return false;
}

/*
 * Class:     org_rocksdb_ByteArrayDeque
 * Method:    removeLastOccurrence0
 * Signature: (JLjava/lang/Object;)Z
 */
jboolean JNICALL Java_org_rocksdb_ByteArrayDeque_removeLastOccurrence0(
    JNIEnv* env, jobject jobj, jlong handle, jobject elem) {
  rocksdb::ExceptionJni::ThrowNew(env,
                                  "java/lang/UnsupportedOperationException",
                                  "");
  return false;
}

/*
 * Class:     org_rocksdb_ByteArrayDeque
 * Method:    remove0
 * Signature: (JLjava/lang/Object;)Z
 */
jboolean JNICALL Java_org_rocksdb_ByteArrayDeque_remove0(JNIEnv* env,
                                                         jobject jobj,
                                                         jlong handle,
                                                         jobject elem) {
  rocksdb::ExceptionJni::ThrowNew(env,
                                  "java/lang/UnsupportedOperationException",
                                  "");
  return false;
}

/*
 * Class:     org_rocksdb_ByteArrayDeque
 * Method:    containsAll0
 * Signature: (JLjava/util/Collection;)Z
 */
jboolean JNICALL Java_org_rocksdb_ByteArrayDeque_containsAll0(JNIEnv* env,
                                                              jobject jobj,
                                                              jlong handle,
                                                              jobject elem) {
  rocksdb::ExceptionJni::ThrowNew(env,
                                  "java/lang/UnsupportedOperationException",
                                  "");
  return false;
}

/*
 * Class:     org_rocksdb_ByteArrayDeque
 * Method:    addAll0
 * Signature: (JLjava/util/Collection;)Z
 */
jboolean JNICALL Java_org_rocksdb_ByteArrayDeque_addAll0(JNIEnv* env,
                                                         jobject jobj,
                                                         jlong handle,
                                                         jobject elem) {
  rocksdb::ExceptionJni::ThrowNew(env,
                                  "java/lang/UnsupportedOperationException",
                                  "");
  return false;
}

/*
 * Class:     org_rocksdb_ByteArrayDeque
 * Method:    removeAll0
 * Signature: (JLjava/util/Collection;)Z
 */
jboolean JNICALL Java_org_rocksdb_ByteArrayDeque_removeAll0(JNIEnv* env,
                                                            jobject jobj,
                                                            jlong handle,
                                                            jobject elem) {
  rocksdb::ExceptionJni::ThrowNew(env,
                                  "java/lang/UnsupportedOperationException",
                                  "");
  return false;
}

/*
 * Class:     org_rocksdb_ByteArrayDeque
 * Method:    retainAll0
 * Signature: (JLjava/util/Collection;)Z
 */
jboolean JNICALL Java_org_rocksdb_ByteArrayDeque_retainAll0(JNIEnv* env,
                                                            jobject jobj,
                                                            jlong handle,
                                                            jobject elem) {
  rocksdb::ExceptionJni::ThrowNew(env,
                                  "java/lang/UnsupportedOperationException",
                                  "");
  return false;
}

/*
 * Class:     org_rocksdb_ByteArrayDeque
 * Method:    clear0
 * Signature: (J)V
 */
void JNICALL Java_org_rocksdb_ByteArrayDeque_clear0(JNIEnv* env, jobject jobj,
                                                    jlong handle) {
  const auto deque = reinterpret_cast<std::deque<std::string> *>(handle);

  deque->clear();
}

/*
 * Class:     org_rocksdb_ByteArrayDeque
 * Method:    contains0
 * Signature: (JLjava/lang/Object;)Z
 */
jboolean JNICALL Java_org_rocksdb_ByteArrayDeque_contains0(JNIEnv* env,
                                                           jobject jobj,
                                                           jlong handle,
                                                           jobject elem) {
  rocksdb::ExceptionJni::ThrowNew(env,
                                  "java/lang/UnsupportedOperationException",
                                  "");
  return false;
}

/*
 * Class:     org_rocksdb_ByteArrayDeque
 * Method:    size0
 * Signature: (J)I
 */
jint JNICALL Java_org_rocksdb_ByteArrayDeque_size0(JNIEnv* env, jobject jobj,
                                                   jlong handle) {
  const auto deque = reinterpret_cast<std::deque<std::string> *>(handle);

  return deque->size();
}

/*
 * Class:     org_rocksdb_ByteArrayDeque
 * Method:    isEmpty0
 * Signature: (J)Z
 */
jboolean JNICALL Java_org_rocksdb_ByteArrayDeque_isEmpty0(JNIEnv* env,
                                                          jobject jobj,
                                                          jlong handle) {
  const auto deque = reinterpret_cast<std::deque<std::string> *>(handle);

  return deque->empty();
}

/*
 * Class:     org_rocksdb_ByteArrayDeque
 * Method:    toArray0
 * Signature: (J)[Ljava/lang/Object;
 */
jobjectArray JNICALL Java_org_rocksdb_ByteArrayDeque_toArray0(JNIEnv* env,
                                                              jobject jobj,
                                                              jlong handle) {
  rocksdb::ExceptionJni::ThrowNew(env,
                                  "java/lang/UnsupportedOperationException",
                                  "");
  return NULL;
}

/*
 * Class:     org_rocksdb_ByteArrayDeque
 * Method:    toArray1
 * Signature: (J[Ljava/lang/Object;)[Ljava/lang/Object;
 */
jobjectArray JNICALL Java_org_rocksdb_ByteArrayDeque_toArray1(
    JNIEnv* env, jobject jobj, jlong handle, jobjectArray array) {
  rocksdb::ExceptionJni::ThrowNew(env,
                                  "java/lang/UnsupportedOperationException",
                                  "");
  return NULL;
}

/*
 * Class:     org_rocksdb_ByteArrayDeque
 * Method:    toString0
 * Signature: (JZ)Ljava/lang/String;
 */
jstring JNICALL Java_org_rocksdb_ByteArrayDeque_toString0(JNIEnv* env,
                                                          jobject jobj,
                                                          jlong handle,
                                                          jboolean hex) {
  rocksdb::ExceptionJni::ThrowNew(env,
                                  "java/lang/UnsupportedOperationException",
                                  "");
  return NULL;
}

/*
 * Class:     org_rocksdb_ByteArrayDeque
 * Method:    disposeInternal
 * Signature: (J)V
 */
void JNICALL Java_org_rocksdb_ByteArrayDeque_disposeInternal(JNIEnv* env,
                                                             jobject jobj,
                                                             jlong handle) {
  delete reinterpret_cast<std::deque<std::string> *>(handle);
}

////////////////////////////// Iterator implementation
/*
 * Class:     org_rocksdb_ByteArrayDeque_Iter
 * Method:    itrhasNext0
 * Signature: (JI)Z
 */
jboolean JNICALL Java_org_rocksdb_ByteArrayDeque_00024Iter_itrhasNext0
(JNIEnv* env, jobject jobj, jlong handle, jint idx) {
  const auto deque = reinterpret_cast<std::deque<std::string> *>(handle);
  auto iter = deque->begin() + idx;
  return (iter >= deque->end());
}

/*
 * Class:     org_rocksdb_ByteArrayDeque_Iter
 * Method:    itrNext0
 * Signature: (JI)[B
 */
jbyteArray JNICALL Java_org_rocksdb_ByteArrayDeque_00024Iter_itrNext0
(JNIEnv* env, jobject jobj, jlong handle, jint idx) {
  const auto deque = reinterpret_cast<std::deque<std::string> *>(handle);
  auto iter = deque->begin() + idx;
  if (iter >= deque->end()) {
    rocksdb::ExceptionJni::ThrowNew(
            env, "java/util/NoSuchElementException",
            "Cannot invoke iterator.next() past the last element!");
        return NULL;
  }

  jbyteArray elem = rocksdb::JniUtil::stdStringToByteArray(env, *iter);
  return elem;
}

/*
 * Class:     org_rocksdb_ByteArrayDeque_Iter
 * Method:    itrRemove0
 * Signature: (JI)V
 */
void JNICALL Java_org_rocksdb_ByteArrayDeque_00024Iter_itrRemove0
(JNIEnv* env, jobject jobj, jlong handle, jint idx) {
  const auto deque = reinterpret_cast<std::deque<std::string> *>(handle);
  auto iter = deque->begin() + idx;
  deque->erase(iter);
}


