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
  const int len = env->GetArrayLength(elem);
  jbyte* ptrData = new jbyte[len];
  env->GetByteArrayRegion(elem, 0, len, ptrData);
  const auto str = std::make_shared<std::string>((char*) ptrData);

  // Add to the front of deque
  const auto deque = reinterpret_cast<std::deque<std::string> *>(handle);
  deque->push_front(*str);
}
