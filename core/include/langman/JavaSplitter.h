
#ifndef _JAVA_SPLITTER_H_
#define _JAVA_SPLITTER_H_
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         JavaSplitter.h
 * Description:  Splitter functions to support different code path for
 *               NSJ 3.0 IEEE float and NSJ2.0 Tandem float
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
#include "common/Platform.h"
#include <jni.h>

class ComDiagsArea;

class JavaSplitter {
 public:
  static jfloat CallStaticFloatMethod(JNIEnv *env, jclass clazz, jmethodID methodID, ...);
  static jdouble CallStaticDoubleMethod(JNIEnv *env, jclass clazz, jmethodID methodID, ...);
  static jfloat CallStaticFloatMethodA(JNIEnv *env, jclass clazz, jmethodID methodID, jvalue *args);
  static jdouble CallStaticDoubleMethodA(JNIEnv *env, jclass clazz, jmethodID methodID, jvalue *args);
  static jfloat CallStaticFloatMethodV(JNIEnv *env, jclass clazz, jmethodID methodID, va_list args);
  static jdouble CallStaticDoubleMethodV(JNIEnv *env, jclass clazz, jmethodID methodID, va_list args);

  static jfloat CallFloatMethod(JNIEnv *env, jobject obj, jmethodID methodID, ...);
  static jdouble CallDoubleMethod(JNIEnv *env, jobject obj, jmethodID methodID, ...);
  static jfloat CallFloatMethodA(JNIEnv *env, jobject obj, jmethodID methodID, jvalue *args);
  static jdouble CallDoubleMethodA(JNIEnv *env, jobject obj, jmethodID methodID, jvalue *args);
  static jfloat CallFloatMethodV(JNIEnv *env, jobject obj, jmethodID methodID, va_list args);
  static jdouble CallDoubleMethodV(JNIEnv *env, jobject obj, jmethodID methodID, va_list args);

  static jfloat CallNonvirtualFloatMethod(JNIEnv *env, jobject obj, jclass clazz, jmethodID methodID, ...);
  static jdouble CallNonvirtualDoubleMethod(JNIEnv *env, jobject obj, jclass clazz, jmethodID methodID, ...);
  static jfloat CallNonvirtualFloatMethodA(JNIEnv *env, jobject obj, jclass clazz, jmethodID methodID, jvalue *args);
  static jdouble CallNonvirtualDoubleMethodA(JNIEnv *env, jobject obj, jclass clazz, jmethodID methodID, jvalue *args);
  static jfloat CallNonvirtualFloatMethodV(JNIEnv *env, jobject obj, jclass clazz, jmethodID methodID, va_list args);
  static jdouble CallNonvirtualDoubleMethodV(JNIEnv *env, jobject obj, jclass clazz, jmethodID methodID, va_list args);

  static int setupVersion(JNIEnv *env, ComDiagsArea *diags);
};

class JavaVirtual {
 public:
  virtual jfloat CallStaticFloatMethodA(JNIEnv *env, jclass clazz, jmethodID methodID, jvalue *args) = 0;
  virtual jdouble CallStaticDoubleMethodA(JNIEnv *env, jclass clazz, jmethodID methodID, jvalue *args) = 0;
  virtual jfloat CallStaticFloatMethodV(JNIEnv *env, jclass clazz, jmethodID methodID, va_list args) = 0;
  virtual jdouble CallStaticDoubleMethodV(JNIEnv *env, jclass clazz, jmethodID methodID, va_list args) = 0;

  virtual jfloat CallFloatMethodA(JNIEnv *env, jobject obj, jmethodID methodID, jvalue *args) = 0;
  virtual jdouble CallDoubleMethodA(JNIEnv *env, jobject obj, jmethodID methodID, jvalue *args) = 0;
  virtual jfloat CallFloatMethodV(JNIEnv *env, jobject obj, jmethodID methodID, va_list args) = 0;
  virtual jdouble CallDoubleMethodV(JNIEnv *env, jobject obj, jmethodID methodID, va_list args) = 0;

  virtual jfloat CallNonvirtualFloatMethodA(JNIEnv *env, jobject obj, jclass clazz, jmethodID methodID,
                                            jvalue *args) = 0;
  virtual jdouble CallNonvirtualDoubleMethodA(JNIEnv *env, jobject obj, jclass clazz, jmethodID methodID,
                                              jvalue *args) = 0;
  virtual jfloat CallNonvirtualFloatMethodV(JNIEnv *env, jobject obj, jclass clazz, jmethodID methodID,
                                            va_list args) = 0;
  virtual jdouble CallNonvirtualDoubleMethodV(JNIEnv *env, jobject obj, jclass clazz, jmethodID methodID,
                                              va_list args) = 0;
};

class JavaNormal : public JavaVirtual {
 public:
  virtual jfloat CallStaticFloatMethodA(JNIEnv *env, jclass clazz, jmethodID methodID, jvalue *args);
  virtual jdouble CallStaticDoubleMethodA(JNIEnv *env, jclass clazz, jmethodID methodID, jvalue *args);
  virtual jfloat CallStaticFloatMethodV(JNIEnv *env, jclass clazz, jmethodID methodID, va_list args);
  virtual jdouble CallStaticDoubleMethodV(JNIEnv *env, jclass clazz, jmethodID methodID, va_list args);

  virtual jfloat CallFloatMethodA(JNIEnv *env, jobject obj, jmethodID methodID, jvalue *args);
  virtual jdouble CallDoubleMethodA(JNIEnv *env, jobject obj, jmethodID methodID, jvalue *args);
  virtual jfloat CallFloatMethodV(JNIEnv *env, jobject obj, jmethodID methodID, va_list args);
  virtual jdouble CallDoubleMethodV(JNIEnv *env, jobject obj, jmethodID methodID, va_list args);

  virtual jfloat CallNonvirtualFloatMethodA(JNIEnv *env, jobject obj, jclass clazz, jmethodID methodID, jvalue *args);
  virtual jdouble CallNonvirtualDoubleMethodA(JNIEnv *env, jobject obj, jclass clazz, jmethodID methodID, jvalue *args);
  virtual jfloat CallNonvirtualFloatMethodV(JNIEnv *env, jobject obj, jclass clazz, jmethodID methodID, va_list args);
  virtual jdouble CallNonvirtualDoubleMethodV(JNIEnv *env, jobject obj, jclass clazz, jmethodID methodID, va_list args);
};

#endif
