// Copyright (c) 2014, Vlad Balan (vlad.gm@gmail.com).  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//
// This file implements the "bridge" between Java and C++
// for rocksdb::MergeOperator.

#include <jni.h>
#include <stdio.h>
#include <stdlib.h>

#include <memory>
#include <string>

#include "include/org_rocksdb_AssociativeMergeOpr.h"
#include "include/org_rocksdb_DirectAssociativeMergeOpr.h"
#include "include/org_rocksdb_MergeOpr.h"
#include "include/org_rocksdb_DirectMergeOpr.h"
#include "include/org_rocksdb_StringAppendOperator.h"
#include "rocksdb/db.h"
#include "rocksdb/memtablerep.h"
#include "rocksdb/merge_operator.h"
#include "rocksdb/options.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/statistics.h"
#include "rocksdb/table.h"
#include "rocksjni/mergeopr_jnicallback.h"
#include "rocksjni/portal.h"
#include "utilities/merge_operators.h"

/*
 * Class:     org_rocksdb_StringAppendOperator
 * Method:    newMergeOperatorHandle
 * Signature: ()J
 */
jlong Java_org_rocksdb_StringAppendOperator_newMergeOperatorHandleImpl
(JNIEnv* env, jobject jobj) {
  std::shared_ptr<rocksdb::MergeOperator> *op =
      new std::shared_ptr<rocksdb::MergeOperator>();
  *op = rocksdb::MergeOperators::CreateFromStringId("stringappend");
  return reinterpret_cast<jlong>(op);
}

/*
 * Class:     org_rocksdb_AbstractMergeOpr
 * Method:    disposeInternal
 * Signature: (J)V
 */
void Java_org_rocksdb_AbstractMergeOpr_disposeInternal
(JNIEnv* env, jobject jobj, jlong handle) {
  delete reinterpret_cast<rocksdb::BaseMergeOprJniCallback*>(handle);
}

/*
 * Class:     org_rocksdb_MergeOpr
 * Method:    createNewMergeOpr0
 * Signature: (J)V
 */
void Java_org_rocksdb_MergeOpr_createNewMergeOpr0
(JNIEnv* env, jobject jobj, jlong mopt_handle) {
  const rocksdb::MergeOprJniCallbackOptions* mopt =
      reinterpret_cast<rocksdb::MergeOprJniCallbackOptions*>(mopt_handle);
  const rocksdb::MergeOprJniCallback* m =
      new rocksdb::MergeOprJniCallback(env, jobj, mopt);
  rocksdb::AbstractMergeOprJni::setHandle(env, jobj, m);
}

/*
 * Class:     org_rocksdb_AssociativeMergeOpr
 * Method:    createNewAssociativeMergeOpr0
 * Signature: (J)V
 */
void JNICALL Java_org_rocksdb_AssociativeMergeOpr_createNewAssociativeMergeOpr0
  (JNIEnv* env, jobject jobj, jlong mopt_handle) {
  const rocksdb::MergeOprJniCallbackOptions* mopt =
      reinterpret_cast<rocksdb::MergeOprJniCallbackOptions*>(mopt_handle);
  const rocksdb::AssociativeMergeOprJniCallback* m = new rocksdb::AssociativeMergeOprJniCallback(
      env, jobj, mopt);
  rocksdb::AbstractAssociativeMergeOprJni::setHandle(env, jobj, m);
}


/*
 * Class:     org_rocksdb_DirectMergeOpr
 * Method:    createNewDirectMergeOpr0
 * Signature: (J)V
 */
void JNICALL Java_org_rocksdb_DirectMergeOpr_createNewDirectMergeOpr0
(JNIEnv* env, jobject jobj, jlong mopt_handle) {
  const rocksdb::MergeOprJniCallbackOptions* mopt =
      reinterpret_cast<rocksdb::MergeOprJniCallbackOptions*>(mopt_handle);
  const rocksdb::DirectMergeOprJniCallback* m =
      new rocksdb::DirectMergeOprJniCallback(env, jobj, mopt);
  rocksdb::AbstractMergeOprJni::setHandle(env, jobj, m);
}

/*
 * Class:     org_rocksdb_DirectAssociativeMergeOpr
 * Method:    createNewDirectAssociativeMergeOpr0
 * Signature: (J)V
 */
void JNICALL Java_org_rocksdb_DirectAssociativeMergeOpr_createNewDirectAssociativeMergeOpr0
  (JNIEnv* env, jobject jobj, jlong mopt_handle) {
  const rocksdb::MergeOprJniCallbackOptions* mopt =
      reinterpret_cast<rocksdb::MergeOprJniCallbackOptions*>(mopt_handle);
  const rocksdb::DirectAssociativeMergeOprJniCallback* m = new rocksdb::DirectAssociativeMergeOprJniCallback(
      env, jobj, mopt);
  rocksdb::AbstractAssociativeMergeOprJni::setHandle(env, jobj, m);
}
