
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         CmpSPUtils.C
 * Description:  This file contains the utility functions provided by arkcmp
 *               for internal stored procedures.
 *
 *
 * Created:      03/16/97
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include <memory.h>

#include "arkcmp/CmpStoredProc.h"

// contents of this file includes the procedures to extract/format the
// fields of data, these functions will be passed into the user developed
// stored procedures to manipulate data.
//
// CmpSPExtractFunc
// CmpSPFormatFunc
// CmpSPKeyValueFunc

SP_HELPER_STATUS CmpSPExtractFunc_(int fieldNo, SP_ROW_DATA rowData, int fieldLen, void *fieldData, int casting) {
  CmpSPExecDataItemInput *inPtr = (CmpSPExecDataItemInput *)rowData;
  int tempNum = (int)fieldNo;
  int tempLen = (int)fieldLen;
  ComDiagsArea *diags = inPtr->SPFuncsDiags();
  if (inPtr->extract(tempNum, (char *)fieldData, tempLen, ((casting == 1) ? TRUE : FALSE), diags) < 0 ||
      diags->getNumber()) {
    // TODO, convert the errors in diags into error code.
    diags->clear();  // to be used for next CmpSPExtractFunc_
    return SP_ERROR_EXTRACT_DATA;
  }
  // test code for assert, check for arkcmp/SPUtil.cpp SP_ERROR_*
  // for detail description.
  //#define ASTRING "TestCMPASSERTEXE"
  // assert( strncmp((char*)fieldData, ASTRING, strlen(ASTRING) != 0 ) ) ;

  return SP_NO_ERROR;
}

SP_HELPER_STATUS CmpSPFormatFunc_(int fieldNo, SP_ROW_DATA rowData, int fieldLen, void *fieldData, int casting) {
  CmpSPExecDataItemReply *replyPtr = (CmpSPExecDataItemReply *)rowData;
  int tempNum = (int)fieldNo;
  int tempLen = (int)fieldLen;
  ComDiagsArea *diags = replyPtr->SPFuncsDiags();
  if (replyPtr->moveOutput(fieldNo, (char *)fieldData, tempLen, ((casting == 1) ? TRUE : FALSE), diags) < 0 ||
      diags->getNumber()) {
    diags->clear();  // to be used for next CmpSPFormatFunc_
    return SP_ERROR_FORMAT_DATA;
  } else
    return SP_NO_ERROR;
}

SP_HELPER_STATUS CmpSPKeyValueFunc_(int keyIndex, SP_KEY_VALUE key, int keyLength, void *keyValue, int casting) {
  return SP_NOT_SUPPORTED_YET;
}

extern "C" {
SP_EXTRACT_FUNCPTR CmpSPExtractFunc = &CmpSPExtractFunc_;
SP_FORMAT_FUNCPTR CmpSPFormatFunc = &CmpSPFormatFunc_;
SP_KEYVALUE_FUNCPTR CmpSPKeyValueFunc = &CmpSPKeyValueFunc_;
}
