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
void JNICALL Java_org_rocksdb_ByteArrayDeque_addLast0
  (JNIEnv* env, jobject jobj, jlong handle, jbyteArray elem) {
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
jbyteArray JNICALL Java_org_rocksdb_ByteArrayDeque_removeFirst0
  (JNIEnv* env, jobject jobj, jlong handle) {
  const auto deque = reinterpret_cast<std::deque<std::string> *>(handle);

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
jbyteArray JNICALL Java_org_rocksdb_ByteArrayDeque_removeLast0
  (JNIEnv* env, jobject jobj, jlong handle) {
  const auto deque = reinterpret_cast<std::deque<std::string> *>(handle);

  std::string& str(deque->back());
  jbyteArray elem = rocksdb::JniUtil::stdStringToByteArray(env, str);

  deque->pop_back();

  return elem;
}
