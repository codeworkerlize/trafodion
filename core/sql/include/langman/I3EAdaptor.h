/**********************************************************************
// @@@ START COPYRIGHT @@@
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// @@@ END COPYRIGHT @@@
**********************************************************************/
#ifndef _I3E_ADAPTOR_H_
#define _I3E_ADAPTOR_H_
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         I3EAdaptor.h
 * Description:  Adaptor functions to handle NSJ 3.0 IEEE float
 *
 *
 * Created:      8/15/02
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */
#include <jni.h>
#include "IntType.h"
class I3EAdaptor {
 public:
  static int CallStaticFloatMethodA(JNIEnv *env, jclass clazz, jmethodID methodID, jvalue *args);
  static long CallStaticDoubleMethodA(JNIEnv *env, jclass clazz, jmethodID methodID, jvalue *args);
  static int CallStaticFloatMethodV(JNIEnv *env, jclass clazz, jmethodID methodID, va_list args);
  static long CallStaticDoubleMethodV(JNIEnv *env, jclass clazz, jmethodID methodID, va_list args);

  static int CallFloatMethodA(JNIEnv *env, jobject obj, jmethodID methodID, jvalue *args);
  static long CallDoubleMethodA(JNIEnv *env, jobject obj, jmethodID methodID, jvalue *args);
  static int CallFloatMethodV(JNIEnv *env, jobject obj, jmethodID methodID, va_list args);
  static long CallDoubleMethodV(JNIEnv *env, jobject obj, jmethodID methodID, va_list args);

  static int CallNonvirtualFloatMethodA(JNIEnv *env, jobject obj, jclass clazz, jmethodID methodID, jvalue *args);
  static long CallNonvirtualDoubleMethodA(JNIEnv *env, jobject obj, jclass clazz, jmethodID methodID, jvalue *args);
  static int CallNonvirtualFloatMethodV(JNIEnv *env, jobject obj, jclass clazz, jmethodID methodID, va_list args);
  static long CallNonvirtualDoubleMethodV(JNIEnv *env, jobject obj, jclass clazz, jmethodID methodID, va_list args);
};
#endif
