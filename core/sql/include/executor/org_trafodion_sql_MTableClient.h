/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class org_trafodion_sql_MTableClient */

#ifndef _Included_org_trafodion_sql_MTableClient
#define _Included_org_trafodion_sql_MTableClient
#ifdef __cplusplus
extern "C" {
#endif
#undef org_trafodion_sql_MTableClient_GET_ROW
#define org_trafodion_sql_MTableClient_GET_ROW 1L
#undef org_trafodion_sql_MTableClient_BATCH_GET
#define org_trafodion_sql_MTableClient_BATCH_GET 2L
#undef org_trafodion_sql_MTableClient_SCAN_FETCH
#define org_trafodion_sql_MTableClient_SCAN_FETCH 3L
/*
 * Class:     org_trafodion_sql_MTableClient
 * Method:    setResultInfo
 * Signature: (J[[B[[B[I[I[J[[B[III)I
 */
JNIEXPORT jint JNICALL Java_org_trafodion_sql_MTableClient_setResultInfo(JNIEnv *, jobject, jlong, jobjectArray,
                                                                         jobjectArray, jintArray, jintArray, jlongArray,
                                                                         jobjectArray, jintArray, jint, jint);

/*
 * Class:     org_trafodion_sql_MTableClient
 * Method:    cleanup
 * Signature: (J)V
 */
JNIEXPORT void JNICALL Java_org_trafodion_sql_MTableClient_cleanup(JNIEnv *, jobject, jlong);

/*
 * Class:     org_trafodion_sql_MTableClient
 * Method:    setJavaObject
 * Signature: (J)I
 */
JNIEXPORT jint JNICALL Java_org_trafodion_sql_MTableClient_setJavaObject(JNIEnv *, jobject, jlong);

#ifdef __cplusplus
}
#endif
#endif
