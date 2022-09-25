// @@@ START COPYRIGHT @@@
// //
// // (C) Copyright 2018 Esgyn Corporation
// //
// // @@@ END COPYRIGHT @@@
//
#include "TrafExtStorageUtils.h"
#include "JavaObjectInterface.h"
//
// ===========================================================================
// ===== Class TrafExtStorageUtils
// ===========================================================================
// array is indexed by enum OFR_RetCode
static const char* const tsuErrorEnumStr[] = 
{
  "Java exception in initStrawScan()"
 ,"Java parameter error in initStrawScan()"
 ,"Java exception in getNextRangeNumStrawScan()"
 ,"Java parameter error in getNextRangeNumStrawScan()"
 ,"Java exception in freeStrawScan()"
 ,"Java parameter error in freeStrawScan()"
};

char* TrafExtStorageUtils::getErrorText(TSU_RetCode pv_errEnum)
{
  if (pv_errEnum < (TSU_RetCode)JOI_LAST)
    return JavaObjectInterface::getErrorText((JOI_RetCode)pv_errEnum);
  else    
    return (char*)tsuErrorEnumStr[pv_errEnum - JOI_LAST];
}

void TrafExtStorageUtils::initMethodNames(JavaMethodInit *javaMethods)
{
   javaMethods[JM_CTOR      ].jm_name      = "<init>";
   javaMethods[JM_CTOR      ].jm_signature = "()V";
   javaMethods[JM_INIT_STRAWSCAN].jm_name      = "initStrawScan";
   javaMethods[JM_INIT_STRAWSCAN].jm_signature = "(Ljava/lang/String;Ljava/lang/String;IZ[Ljava/lang/Object;)Ljava/lang/String;";
   javaMethods[JM_GETNEXTRANGENUM_STRAWSCAN].jm_name      = "getNextRangeNumStrawScan";
   javaMethods[JM_GETNEXTRANGENUM_STRAWSCAN].jm_signature = "(Ljava/lang/String;Ljava/lang/String;IIJIIZ)I";
   javaMethods[JM_FREE_STRAWSCAN].jm_name      = "freeStrawScan";
   javaMethods[JM_FREE_STRAWSCAN].jm_signature = "(Ljava/lang/String;I)Ljava/lang/String;";
   childJavaMethods_ = javaMethods;
   return;
}

TSU_RetCode TrafExtStorageUtils::initStrawScan(char* webServers, char* queryId, Lng32 explainNodeId, bool isFactTable, 
        NAText* entries, CollIndex entriesLength)
{
  QRLogger::log(CAT_SQL_HDFS_ORC_FILE_READER, LL_DEBUG,
                "TrafExtStorageUtils::initStrawScan(queryId=%s,explainNodeId=%d, entriesLength=%d) called.",
                queryId,
		explainNodeId,
                entriesLength);
  TSU_RetCode lv_retcode = TSU_OK;
  jstring   js_webServers = NULL;
  jstring   js_queryId = NULL;
  jint      ji_explainNodeId = explainNodeId;
  jboolean  jb_isFactTable = isFactTable;
  jobjectArray jor_entries = NULL;
  jstring   jresult = NULL;
  if (initJNIEnv() != JOI_OK)
     return TSU_ERROR_INITSTRAWSCAN_PARAM;
  js_queryId = jenv_->NewStringUTF(queryId);
  js_webServers = jenv_->NewStringUTF(webServers);
  if (js_queryId == NULL || js_webServers == NULL) {
    lv_retcode = TSU_ERROR_INITSTRAWSCAN_PARAM;
    goto fn_exit;
  }
  if (entries && entriesLength > 0)
  {
    jor_entries = convertToStringObjectArray(entries, entriesLength);
    if (jor_entries == NULL) {
        lv_retcode = TSU_ERROR_INITSTRAWSCAN_PARAM;
	    goto fn_exit;
    }
  } else {
    lv_retcode = TSU_ERROR_INITSTRAWSCAN_PARAM;
    goto fn_exit;
  }
  tsRecentJMFromJNI = childJavaMethods_[JM_INIT_STRAWSCAN].jm_full_name;
  jresult = (jstring)jenv_->CallObjectMethod(javaObj_,
                             childJavaMethods_[JM_INIT_STRAWSCAN].methodID,
                             js_webServers,
                             js_queryId,
                             ji_explainNodeId,
                             jb_isFactTable,
                             jor_entries);
  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails(__FILE__,__LINE__,"TrafExtStorageUtils::initStrawScan()");
    lv_retcode = TSU_ERROR_INITSTRAWSCAN_EXCEPTION;
    goto fn_exit;
  }
 fn_exit:
  jenv_->PopLocalFrame(NULL);
  return lv_retcode;
}

TSU_RetCode TrafExtStorageUtils::getNextRangeNumStrawScan(char* webServers,
       char* queryId,
       Lng32 explainNodeId,
       Lng32 sequenceNb,
       ULng32 executionCount,
       Lng32 espNb,
       Lng32 nodeId,
       bool isFactTable,
       Lng32& nextRangeNum)
{
  QRLogger::log(CAT_SQL_HDFS_ORC_FILE_READER, LL_DEBUG,
                "TrafExtStorageUtils::getNextRangeNumStrawScan(queryId=%s,explainNodeId=%d, sn=%d, ex=%ld, esp=%d, nid=%d) called.",
                queryId,
		explainNodeId,
	    	sequenceNb,
	    	executionCount,
		espNb,
		nodeId);
  TSU_RetCode lv_retcode = TSU_OK;
  jstring   js_webServers = NULL;
  jstring   js_queryId = NULL;
  jint      ji_explainNodeId = explainNodeId;
  jint      ji_sequenceNb= sequenceNb;
  jlong     jl_executionCount = (long)executionCount;
  jint      ji_espNb= espNb;
  jint      ji_nodeId = nodeId;
  jboolean  jb_isFactTable = isFactTable;
  if (initJNIEnv() != JOI_OK)
     return TSU_ERROR_GETNEXTRANGENUMSTRAWSCAN_PARAM;
  js_queryId = jenv_->NewStringUTF(queryId);
  js_webServers = jenv_->NewStringUTF(webServers);
  if (js_queryId == NULL || js_webServers == NULL) {
    lv_retcode = TSU_ERROR_GETNEXTRANGENUMSTRAWSCAN_PARAM;
    goto fn_exit;
  }
  tsRecentJMFromJNI = childJavaMethods_[JM_GETNEXTRANGENUM_STRAWSCAN].jm_full_name;
  nextRangeNum = jenv_->CallIntMethod(javaObj_,
                                      childJavaMethods_[JM_GETNEXTRANGENUM_STRAWSCAN].methodID,
                             js_webServers,
                             js_queryId,
                             ji_explainNodeId,
                             ji_sequenceNb,
                             jl_executionCount,
                             ji_espNb,
                             ji_nodeId,
                             jb_isFactTable);
  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails( __FILE__, __LINE__,"TrafExtStorageUtils::getNextRangeNumStrawScan()");
    lv_retcode = TSU_ERROR_GETNEXTRANGENUMSTRAWSCAN_EXCEPTION;
    goto fn_exit;
  }
 fn_exit:
  jenv_->PopLocalFrame(NULL);
  return lv_retcode;
}

TSU_RetCode TrafExtStorageUtils::freeStrawScan(char* queryId, Lng32 explainNodeId )
{
  QRLogger::log(CAT_SQL_HDFS_ORC_FILE_READER, LL_DEBUG,
                "TrafExtStorageUtils::freeStrawScan(queryId=%s,explainNodeId=%d) called.",
                queryId,
       explainNodeId);
  TSU_RetCode lv_retcode = TSU_OK;
  jstring   js_queryId = NULL;
  jint      ji_explainNodeId = explainNodeId;
  jstring   jresult = NULL;
  if (initJNIEnv() != JOI_OK)
     return TSU_ERROR_FREESTRAWSCAN_PARAM;
  js_queryId = jenv_->NewStringUTF(queryId);
  if (js_queryId == NULL) {
    lv_retcode = TSU_ERROR_FREESTRAWSCAN_PARAM;
    goto fn_exit;
  }
  tsRecentJMFromJNI = childJavaMethods_[JM_FREE_STRAWSCAN].jm_full_name;
  jresult = (jstring)jenv_->CallObjectMethod(javaObj_,
                                             childJavaMethods_[JM_FREE_STRAWSCAN].methodID,
                                             js_queryId, ji_explainNodeId);
  if (jenv_->ExceptionCheck())
  {
    getExceptionDetails( __FILE__, __LINE__,"TrafExtStorageUtils::freeStrawScan()");
    lv_retcode = TSU_ERROR_FREESTRAWSCAN_EXCEPTION;
    goto fn_exit;
  }
 fn_exit:
  jenv_->PopLocalFrame(NULL);
  return lv_retcode;
}
