// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// This file implements the callback "bridge" between Java and C++ for
// rocksdb::Comparator.

#include "rocksjni/associative_mergeopr_jnicallback.h"
#include "rocksjni/portal.h"

namespace rocksdb {
BaseAssociativeMergeOprJniCallback::BaseAssociativeMergeOprJniCallback(
    JNIEnv* env, jobject jAssociativeMergeOpr,
    const AssociativeMergeOprJniCallbackOptions* mopt) :
    mtx_merge(new port::Mutex(mopt->use_adaptive_mutex)) {
  // Note: merge method may be accessed by multiple threads,
  // so we ref the jvm not the env
  const jint rs = env->GetJavaVM(&m_jvm);
  assert(rs == JNI_OK);

  // Note: we want to access the Java AssociativeMergeOpr instance
  // across multiple method calls, so we create a global ref
  m_jAssociativeMergeOpr = env->NewGlobalRef(jAssociativeMergeOpr);

  // Note: The name of an AssociativeMergeOpr will not change during it's lifetime,
  // so we cache it in a global var
  jmethodID jNameMethodId = AbstractMergeOprJni::getNameMethodId(env);
  jstring jsName = (jstring) env->CallObjectMethod(m_jAssociativeMergeOpr,
                                                   jNameMethodId);
  m_name = JniUtil::copyString(env, jsName);  // also releases jsName

  m_jMergeMethodId = AbstractMergeOprJni::getMergeMethodId(env);
}

/**
 * Attach/Get a JNIEnv for the current native thread
 */
JNIEnv* BaseAssociativeMergeOprJniCallback::getJniEnv() const {
  JNIEnv *env;
  jint rs = m_jvm->AttachCurrentThread(reinterpret_cast<void **>(&env), NULL);
  assert(rs == JNI_OK);
  return env;
}

const char* BaseAssociativeMergeOprJniCallback::Name() const {
  return m_name.c_str();
}

bool BaseAssociativeMergeOprJniCallback::Merge(const Slice& key,
                                               const Slice* existing_value,
                                               const Slice& value,
                                               std::string* new_value,
                                               Logger* logger) const {
  JNIEnv* m_env = getJniEnv();

  // TODO(pshareghi): slice objects can potentially be cached using thread
  // local variables to avoid locking. Could make this configurable depending
  // on performance.
  mtx_merge->Lock();

  AbstractSliceJni::setHandle(m_env, m_jKeySlice, &key);
  AbstractSliceJni::setHandle(m_env, m_jExistingValueSlice, existing_value);
  AbstractSliceJni::setHandle(m_env, m_jValueSlice, &value);

  bool success;

  jbyteArray jNewValue = (jbyteArray) m_env->CallObjectMethod(
      m_jAssociativeMergeOpr, m_jMergeMethodId, m_jKeySlice,
      m_jExistingValueSlice, m_jValueSlice);

  // Check if an exception occurred
  jthrowable exception = m_env->ExceptionOccurred();

  if (exception == NULL) {
    // There was no exception. Great! Make sure merge result is not NULL
    if (jNewValue != NULL) {
      int len = m_env->GetArrayLength(jNewValue);
      char* cppNewValue = new char[len];
      m_env->GetByteArrayRegion(jNewValue, 0, len, (jbyte*) cppNewValue);
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

    m_env->ExceptionDescribe();
    m_env->ExceptionClear();
    RocksDBExceptionJni::ThrowNew(
        m_env, "Java exception happened during merge java callback!",
        exception);
  }

  // Finally, unlock and detach
  mtx_merge->Unlock();
  m_jvm->DetachCurrentThread();

  return success;
}

BaseAssociativeMergeOprJniCallback::~BaseAssociativeMergeOprJniCallback() {
  JNIEnv* m_env = getJniEnv();

  m_env->DeleteGlobalRef(m_jAssociativeMergeOpr);

  // Note: do not need to explicitly detach, as this function is effectively
  // called from the Java class's disposeInternal method, and so already
  // has an attached thread, getJniEnv above is just a no-op Attach to get
  // the env jvm->DetachCurrentThread();
}

AssoicativeMergeOprJniCallback::AssoicativeMergeOprJniCallback(
    JNIEnv* env, jobject jAssociativeMergeOpr,
    const AssociativeMergeOprJniCallbackOptions* mopt) :
    BaseAssociativeMergeOprJniCallback(env, jAssociativeMergeOpr, mopt) {
  m_jKeySlice = env->NewGlobalRef(SliceJni::construct0(env));
  m_jExistingValueSlice = env->NewGlobalRef(SliceJni::construct0(env));
  m_jValueSlice = env->NewGlobalRef(SliceJni::construct0(env));
}

AssoicativeMergeOprJniCallback::~AssoicativeMergeOprJniCallback() {
  JNIEnv* m_env = getJniEnv();
  m_env->DeleteGlobalRef(m_jKeySlice);
  m_env->DeleteGlobalRef(m_jExistingValueSlice);
  m_env->DeleteGlobalRef(m_jValueSlice);
}

DirectAssociativeMergeOprJniCallback::DirectAssociativeMergeOprJniCallback(
    JNIEnv* env, jobject jAssociativeMergeOpr,
    const AssociativeMergeOprJniCallbackOptions* mopt) :
    BaseAssociativeMergeOprJniCallback(env, jAssociativeMergeOpr, mopt) {
  m_jKeySlice = env->NewGlobalRef(DirectSliceJni::construct0(env));
  m_jExistingValueSlice = env->NewGlobalRef(DirectSliceJni::construct0(env));
  m_jValueSlice = env->NewGlobalRef(DirectSliceJni::construct0(env));
}

DirectAssociativeMergeOprJniCallback::~DirectAssociativeMergeOprJniCallback() {
  JNIEnv* m_env = getJniEnv();
  m_env->DeleteGlobalRef(m_jKeySlice);
  m_env->DeleteGlobalRef(m_jExistingValueSlice);
  m_env->DeleteGlobalRef(m_jValueSlice);
}
}  // namespace rocksdb
