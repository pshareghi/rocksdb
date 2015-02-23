// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// This file implements the callback "bridge" between Java and C++ for
// rocksdb::Comparator.

#include "rocksjni/mergeopr_jnicallback.h"
#include "rocksjni/portal.h"

namespace rocksdb {
BaseMergeOprJniCallback::BaseMergeOprJniCallback(
    JNIEnv* env, jobject jMergeOpr,
    const MergeOprJniCallbackOptions* mopt) :
    mtx_merge(new port::Mutex(mopt->use_adaptive_mutex)) {
  // Note: merge method may be accessed by multiple threads,
  // so we ref the jvm not the env
  const jint rs = env->GetJavaVM(&m_jvm);
  assert(rs == JNI_OK);

  // Note: we want to access the Java MergeOpr instance
  // across multiple method calls, so we create a global ref
  m_jMergeOpr = env->NewGlobalRef(jMergeOpr);

  // Note: The name of a MergeOpr will not change during it's
  // lifetime, so we cache it in a global var
  jmethodID jNameMethodId = AbstractMergeOprJni::getNameMethodId(env);
  jstring jsName = (jstring) env->CallObjectMethod(m_jMergeOpr,
                                                   jNameMethodId);
  m_name = JniUtil::copyString(env, jsName);  // also releases jsName

  m_jFullMergeMethodId = AbstractMergeOprJni::getFullMergeMethodId(env);
  m_jPartialMergeMethodId = AbstractMergeOprJni::getPartialMergeMethodId(env);
  m_jPartialMergeMultiMethodId =
      AbstractMergeOprJni::getPartialMergeMultiMethodId(env);
}

/**
 * Attach/Get a JNIEnv for the current native thread
 */
JNIEnv* BaseMergeOprJniCallback::getJniEnv() const {
  JNIEnv *env;
  jint rs = m_jvm->AttachCurrentThread(reinterpret_cast<void **>(&env), NULL);
  assert(rs == JNI_OK);
  return env;
}

const char* BaseMergeOprJniCallback::Name() const {
  return m_name.c_str();
}

bool BaseMergeOprJniCallback::FullMerge(
    const Slice& key, const Slice* existing_value,
    const std::deque<std::string>& operand_list, std::string* new_value,
    Logger* logger) const {
  JNIEnv* env = getJniEnv();

  // TODO(pshareghi): slice objects can potentially be cached using thread
  // local variables to avoid locking. Could make this configurable depending
  // on performance.
  mtx_merge->Lock();

  AbstractSliceJni::setHandle(env, m_jKeySlice, &key);
  AbstractSliceJni::setHandle(env, m_jExistingValueSlice, existing_value);
  ByteArrayDequeJni::setHandle(env, m_jByteArrayOperandList, &operand_list);

  bool success;

  jbyteArray jNewValue = (jbyteArray) env->CallObjectMethod(
      m_jMergeOpr, m_jFullMergeMethodId, m_jKeySlice,
      m_jExistingValueSlice, m_jByteArrayOperandList);

  // Check if an exception occurred
  jthrowable exception = env->ExceptionOccurred();

  if (exception == NULL) {
    // There was no exception. Great! Make sure merge result is not NULL
    if (jNewValue != NULL) {
      int len = env->GetArrayLength(jNewValue);
      char* cppNewValue = new char[len];
      env->GetByteArrayRegion(jNewValue, 0, len,
          reinterpret_cast<jbyte*>(cppNewValue));
      new_value->assign(cppNewValue);
      delete cppNewValue;
      success = true;
    } else {
      // Merge result was NULL, merge failed
      new_value->clear();
      success = false;
    }

  } else {
    // An exception occurred, re-throw as RocksDBException
    success = false;

    env->ExceptionDescribe();
    env->ExceptionClear();
    RocksDBExceptionJni::ThrowNew(
        env, "Java exception happened during merge java callback!",
        exception);
  }

  // Finally, unlock and detach
  mtx_merge->Unlock();
  m_jvm->DetachCurrentThread();

  return success;
}

bool BaseMergeOprJniCallback::PartialMerge(const Slice& key,
                                           const Slice& left_operand,
                                           const Slice& right_operand,
                                           std::string* new_value,
                                           Logger* logger) const {
  JNIEnv* env = getJniEnv();

  // TODO(pshareghi): slice objects can potentially be cached using thread
  // local variables to avoid locking. Could make this configurable depending
  // on performance.
  mtx_merge->Lock();

  AbstractSliceJni::setHandle(env, m_jKeySlice, &key);
  AbstractSliceJni::setHandle(env, m_jLeftOperand, &left_operand);
  AbstractSliceJni::setHandle(env, m_jRightOperand, &right_operand);

  bool success;

  jbyteArray jNewValue = (jbyteArray) env->CallObjectMethod(
      m_jMergeOpr, m_jPartialMergeMethodId, m_jKeySlice,
      m_jLeftOperand, m_jRightOperand);

  // Check if an exception occurred
  jthrowable exception = env->ExceptionOccurred();

  if (exception == NULL) {
    // There was no exception. Great! Make sure merge result is not NULL
    if (jNewValue != NULL) {
      int len = env->GetArrayLength(jNewValue);
      char* cppNewValue = new char[len];
      env->GetByteArrayRegion(jNewValue, 0, len,
          reinterpret_cast<jbyte*>(cppNewValue));
      new_value->assign(cppNewValue);
      delete cppNewValue;
      success = true;
    } else {
      // Merge result was NULL, merge failed
      new_value->clear();
      success = false;
    }

  } else {
    // An exception occurred, re-throw as RocksDBException
    success = false;

    env->ExceptionDescribe();
    env->ExceptionClear();
    RocksDBExceptionJni::ThrowNew(
        env, "Java exception happened during merge java callback!",
        exception);
  }

  // Finally, unlock and detach
  mtx_merge->Unlock();
  m_jvm->DetachCurrentThread();

  return success;
}

bool BaseMergeOprJniCallback::PartialMergeMulti(
    const Slice& key, const std::deque<Slice>& operand_list,
    std::string* new_value, Logger* logger) const {
  JNIEnv* env = getJniEnv();

  // TODO(pshareghi): slice objects can potentially be cached using thread
  // local variables to avoid locking. Could make this configurable depending
  // on performance.
  mtx_merge->Lock();

  AbstractSliceJni::setHandle(env, m_jKeySlice, &key);
  setSliceOperandListHandle(env, operand_list);

  bool success;

  jbyteArray jNewValue = (jbyteArray) env->CallObjectMethod(
      m_jMergeOpr, m_jPartialMergeMethodId, m_jKeySlice,
      m_jLeftOperand, m_jRightOperand);

  // Check if an exception occurred
  jthrowable exception = env->ExceptionOccurred();

  if (exception == NULL) {
    // There was no exception. Great! Make sure merge result is not NULL
    if (jNewValue != NULL) {
      int len = env->GetArrayLength(jNewValue);
      char* cppNewValue = new char[len];
      env->GetByteArrayRegion(jNewValue, 0, len,
          reinterpret_cast<jbyte*>(cppNewValue));
      new_value->assign(cppNewValue);
      delete cppNewValue;
      success = true;
    } else {
      // Merge result was NULL, merge failed
      new_value->clear();
      success = false;
    }

  } else {
    // An exception occurred, re-throw as RocksDBException
    success = false;

    env->ExceptionDescribe();
    env->ExceptionClear();
    RocksDBExceptionJni::ThrowNew(
        env, "Java exception happened during merge java callback!",
        exception);
  }

  // Finally, unlock and detach
  mtx_merge->Unlock();
  m_jvm->DetachCurrentThread();

  return success;
}

BaseMergeOprJniCallback::~BaseMergeOprJniCallback() {
  JNIEnv* env = getJniEnv();

  env->DeleteGlobalRef(m_jMergeOpr);

  // Note: do not need to explicitly detach, as this function is effectively
  // called from the Java class's disposeInternal method, and so already
  // has an attached thread, getJniEnv above is just a no-op Attach to get
  // the env jvm->DetachCurrentThread();
}

MergeOprJniCallback::MergeOprJniCallback(JNIEnv* env, jobject jMergeOpr,
                                         const MergeOprJniCallbackOptions* mopt) :
    BaseMergeOprJniCallback(env, jMergeOpr, mopt) {
  m_jKeySlice = env->NewGlobalRef(SliceJni::construct0(env));
  m_jExistingValueSlice = env->NewGlobalRef(SliceJni::construct0(env));
  m_jByteArrayOperandList = env->NewGlobalRef(
      ByteArrayDequeJni::construct0(env));

  m_jLeftOperand = env->NewGlobalRef(SliceJni::construct0(env));
  m_jRightOperand = env->NewGlobalRef(SliceJni::construct0(env));

  m_jSliceOperandList = newSliceOperandList(env);
}

MergeOprJniCallback::~MergeOprJniCallback() {
  JNIEnv* env = getJniEnv();
  env->DeleteGlobalRef(m_jKeySlice);

  env->DeleteGlobalRef(m_jExistingValueSlice);
  env->DeleteGlobalRef(m_jByteArrayOperandList);

  env->DeleteGlobalRef(m_jLeftOperand);
  env->DeleteGlobalRef(m_jRightOperand);
  env->DeleteGlobalRef(m_jSliceOperandList);
}

void MergeOprJniCallback::setSliceOperandListHandle(JNIEnv* env, const std::deque<Slice>& operand_list) const {
  //TODO: implement
}

jobject MergeOprJniCallback::newSliceOperandList(JNIEnv* env) const {
  //TODO: implement
  return NULL;
}

DirectMergeOprJniCallback::DirectMergeOprJniCallback(
    JNIEnv* env, jobject jMergeOpr, const MergeOprJniCallbackOptions* mopt) :
    BaseMergeOprJniCallback(env, jMergeOpr, mopt) {
  m_jKeySlice = env->NewGlobalRef(DirectSliceJni::construct0(env));
  m_jExistingValueSlice = env->NewGlobalRef(DirectSliceJni::construct0(env));
  m_jByteArrayOperandList = env->NewGlobalRef(
      ByteArrayDequeJni::construct0(env));

  m_jLeftOperand = env->NewGlobalRef(DirectSliceJni::construct0(env));
  m_jRightOperand = env->NewGlobalRef(DirectSliceJni::construct0(env));

  m_jSliceOperandList = newSliceOperandList(env);
}

DirectMergeOprJniCallback::~DirectMergeOprJniCallback() {
  JNIEnv* env = getJniEnv();
  env->DeleteGlobalRef(m_jKeySlice);
  env->DeleteGlobalRef(m_jExistingValueSlice);
  env->DeleteGlobalRef(m_jByteArrayOperandList);

  env->DeleteGlobalRef(m_jLeftOperand);
  env->DeleteGlobalRef(m_jRightOperand);

  env->DeleteGlobalRef(m_jSliceOperandList);
}

void DirectMergeOprJniCallback::setSliceOperandListHandle(JNIEnv* env, const std::deque<Slice>& operand_list) const {
  //TODO: implement
}

jobject DirectMergeOprJniCallback::newSliceOperandList(JNIEnv* env) const {
  //TODO: implement
  return NULL;
}
}  // namespace rocksdb
