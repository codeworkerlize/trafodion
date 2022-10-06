
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         I3EAdaptor.cpp
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

// This Adaptor module has to be compiled with -WIEEE_float flag in c89.
// The current NT-to-NSK c89 we are using does not support this flag.
// Therefore, this module is compiled on NSK and the produced object file
// I3EAdaptor.o is checked into w:/langman
#include "I3EAdaptor.h"

#include <jni.h>

#include "IntType.h"

int I3EAdaptor::CallStaticFloatMethodA(JNIEnv *env, jclass clazz, jmethodID methodID, jvalue *args) {
  jfloat result = env->CallStaticFloatMethodA(clazz, methodID, args);
  return *((int *)&result);
}

long I3EAdaptor::CallStaticDoubleMethodA(JNIEnv *env, jclass clazz, jmethodID methodID, jvalue *args) {
  jdouble result = env->CallStaticDoubleMethodA(clazz, methodID, args);
  return *((long *)&result);
}

int I3EAdaptor::CallStaticFloatMethodV(JNIEnv *env, jclass clazz, jmethodID methodID, va_list args) {
  jfloat result = env->CallStaticFloatMethodV(clazz, methodID, args);
  return *((int *)&result);
}

long I3EAdaptor::CallStaticDoubleMethodV(JNIEnv *env, jclass clazz, jmethodID methodID, va_list args) {
  jdouble result = env->CallStaticDoubleMethodV(clazz, methodID, args);
  return *((long *)&result);
}

int I3EAdaptor::CallFloatMethodA(JNIEnv *env, jobject obj, jmethodID methodID, jvalue *args) {
  jfloat result = env->CallFloatMethodA(obj, methodID, args);
  return *((int *)&result);
}

long I3EAdaptor::CallDoubleMethodA(JNIEnv *env, jobject obj, jmethodID methodID, jvalue *args) {
  jdouble result = env->CallDoubleMethodA(obj, methodID, args);
  return *((long *)&result);
}

int I3EAdaptor::CallFloatMethodV(JNIEnv *env, jobject obj, jmethodID methodID, va_list args) {
  jfloat result = env->CallFloatMethodV(obj, methodID, args);
  return *((int *)&result);
}

long I3EAdaptor::CallDoubleMethodV(JNIEnv *env, jobject obj, jmethodID methodID, va_list args) {
  jdouble result = env->CallDoubleMethodV(obj, methodID, args);
  return *((long *)&result);
}

int I3EAdaptor::CallNonvirtualFloatMethodA(JNIEnv *env, jobject obj, jclass clazz, jmethodID methodID, jvalue *args) {
  jfloat result = env->CallNonvirtualFloatMethodA(obj, clazz, methodID, args);
  return *((int *)&result);
}

long I3EAdaptor::CallNonvirtualDoubleMethodA(JNIEnv *env, jobject obj, jclass clazz, jmethodID methodID, jvalue *args) {
  jdouble result = env->CallNonvirtualDoubleMethodA(obj, clazz, methodID, args);
  return *((long *)&result);
}

int I3EAdaptor::CallNonvirtualFloatMethodV(JNIEnv *env, jobject obj, jclass clazz, jmethodID methodID, va_list args) {
  jfloat result = env->CallNonvirtualFloatMethodV(obj, clazz, methodID, args);
  return *((int *)&result);
}

long I3EAdaptor::CallNonvirtualDoubleMethodV(JNIEnv *env, jobject obj, jclass clazz, jmethodID methodID, va_list args) {
  jdouble result = env->CallNonvirtualDoubleMethodV(obj, clazz, methodID, args);
  return *((long *)&result);
}
