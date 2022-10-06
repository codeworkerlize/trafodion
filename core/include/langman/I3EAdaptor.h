
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
