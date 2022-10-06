/* DO NOT EDIT THIS FILE - it is machine generated */
#include <jni.h>
/* Header for class org_trafodion_sql_OrcFileVectorReader */

#ifndef _Included_org_trafodion_sql_OrcFileVectorReader
#define _Included_org_trafodion_sql_OrcFileVectorReader
#ifdef __cplusplus
extern "C" {
#endif
#undef org_trafodion_sql_OrcFileVectorReader_HIVE_UNKNOWN_TYPE
#define org_trafodion_sql_OrcFileVectorReader_HIVE_UNKNOWN_TYPE 0L
#undef org_trafodion_sql_OrcFileVectorReader_HIVE_BOOLEAN_TYPE
#define org_trafodion_sql_OrcFileVectorReader_HIVE_BOOLEAN_TYPE 1L
#undef org_trafodion_sql_OrcFileVectorReader_HIVE_BYTE_TYPE
#define org_trafodion_sql_OrcFileVectorReader_HIVE_BYTE_TYPE 2L
#undef org_trafodion_sql_OrcFileVectorReader_HIVE_SHORT_TYPE
#define org_trafodion_sql_OrcFileVectorReader_HIVE_SHORT_TYPE 3L
#undef org_trafodion_sql_OrcFileVectorReader_HIVE_INT_TYPE
#define org_trafodion_sql_OrcFileVectorReader_HIVE_INT_TYPE 4L
#undef org_trafodion_sql_OrcFileVectorReader_HIVE_LONG_TYPE
#define org_trafodion_sql_OrcFileVectorReader_HIVE_LONG_TYPE 5L
#undef org_trafodion_sql_OrcFileVectorReader_HIVE_FLOAT_TYPE
#define org_trafodion_sql_OrcFileVectorReader_HIVE_FLOAT_TYPE 6L
#undef org_trafodion_sql_OrcFileVectorReader_HIVE_DOUBLE_TYPE
#define org_trafodion_sql_OrcFileVectorReader_HIVE_DOUBLE_TYPE 7L
#undef org_trafodion_sql_OrcFileVectorReader_HIVE_DECIMAL_TYPE
#define org_trafodion_sql_OrcFileVectorReader_HIVE_DECIMAL_TYPE 8L
#undef org_trafodion_sql_OrcFileVectorReader_HIVE_CHAR_TYPE
#define org_trafodion_sql_OrcFileVectorReader_HIVE_CHAR_TYPE 9L
#undef org_trafodion_sql_OrcFileVectorReader_HIVE_VARCHAR_TYPE
#define org_trafodion_sql_OrcFileVectorReader_HIVE_VARCHAR_TYPE 10L
#undef org_trafodion_sql_OrcFileVectorReader_HIVE_STRING_TYPE
#define org_trafodion_sql_OrcFileVectorReader_HIVE_STRING_TYPE 11L
#undef org_trafodion_sql_OrcFileVectorReader_HIVE_BINARY_TYPE
#define org_trafodion_sql_OrcFileVectorReader_HIVE_BINARY_TYPE 12L
#undef org_trafodion_sql_OrcFileVectorReader_HIVE_DATE_TYPE
#define org_trafodion_sql_OrcFileVectorReader_HIVE_DATE_TYPE 13L
#undef org_trafodion_sql_OrcFileVectorReader_HIVE_TIMESTAMP_TYPE
#define org_trafodion_sql_OrcFileVectorReader_HIVE_TIMESTAMP_TYPE 14L
#undef org_trafodion_sql_OrcFileVectorReader_UNKNOWN_OPER
#define org_trafodion_sql_OrcFileVectorReader_UNKNOWN_OPER 0L
#undef org_trafodion_sql_OrcFileVectorReader_STARTAND
#define org_trafodion_sql_OrcFileVectorReader_STARTAND 1L
#undef org_trafodion_sql_OrcFileVectorReader_STARTOR
#define org_trafodion_sql_OrcFileVectorReader_STARTOR 2L
#undef org_trafodion_sql_OrcFileVectorReader_STARTNOT
#define org_trafodion_sql_OrcFileVectorReader_STARTNOT 3L
#undef org_trafodion_sql_OrcFileVectorReader_END
#define org_trafodion_sql_OrcFileVectorReader_END 4L
#undef org_trafodion_sql_OrcFileVectorReader_EQUALS
#define org_trafodion_sql_OrcFileVectorReader_EQUALS 5L
#undef org_trafodion_sql_OrcFileVectorReader_LESSTHAN
#define org_trafodion_sql_OrcFileVectorReader_LESSTHAN 6L
#undef org_trafodion_sql_OrcFileVectorReader_LESSTHANEQUALS
#define org_trafodion_sql_OrcFileVectorReader_LESSTHANEQUALS 7L
#undef org_trafodion_sql_OrcFileVectorReader_ISNULL
#define org_trafodion_sql_OrcFileVectorReader_ISNULL 8L
#undef org_trafodion_sql_OrcFileVectorReader_IN
#define org_trafodion_sql_OrcFileVectorReader_IN 9L
#undef org_trafodion_sql_OrcFileVectorReader_BF
#define org_trafodion_sql_OrcFileVectorReader_BF 10L
#undef org_trafodion_sql_OrcFileVectorReader_JGREG
#define org_trafodion_sql_OrcFileVectorReader_JGREG 588829L
#undef org_trafodion_sql_OrcFileVectorReader_HALFSECOND
#define org_trafodion_sql_OrcFileVectorReader_HALFSECOND 0.5
#undef org_trafodion_sql_OrcFileVectorReader_SYNC_REPL
#define org_trafodion_sql_OrcFileVectorReader_SYNC_REPL 1L
#undef org_trafodion_sql_OrcFileVectorReader_INCR_BACKUP
#define org_trafodion_sql_OrcFileVectorReader_INCR_BACKUP 2L
#undef org_trafodion_sql_OrcFileVectorReader_ASYNC_OPER
#define org_trafodion_sql_OrcFileVectorReader_ASYNC_OPER 4L
#undef org_trafodion_sql_OrcFileVectorReader_USE_REGION_XN
#define org_trafodion_sql_OrcFileVectorReader_USE_REGION_XN 8L
#undef org_trafodion_sql_OrcFileVectorReader_USE_TREX
#define org_trafodion_sql_OrcFileVectorReader_USE_TREX 16L
#undef org_trafodion_sql_OrcFileVectorReader_NO_CONFLICT_CHECK
#define org_trafodion_sql_OrcFileVectorReader_NO_CONFLICT_CHECK 32L
/*
 * Class:     org_trafodion_sql_OrcFileVectorReader
 * Method:    setResultInfo
 * Signature: (JZI[I[II[Ljava/lang/Object;[Ljava/lang/Object;[Ljava/lang/Object;[Z[Ljava/lang/Object;[Z)V
 */
JNIEXPORT void JNICALL Java_org_trafodion_sql_OrcFileVectorReader_setResultInfo(JNIEnv *, jobject, jlong, jboolean,
                                                                                jint, jintArray, jintArray, jint,
                                                                                jobjectArray, jobjectArray,
                                                                                jobjectArray, jbooleanArray,
                                                                                jobjectArray, jbooleanArray);

#ifdef __cplusplus
}
#endif
#endif