
/* -*-C++-*-
******************************************************************************
*
* File:         RuException.cpp
* Description:  Implementation of class CRUException
*
* Created:      12/29/1999
* Language:     C++
*
*
******************************************************************************
*/

#include "RuException.h"
#include "uofsIpcMessageTranslator.h"

//--------------------------------------------------------------------------//
//	CRUException::StoreData()
//--------------------------------------------------------------------------//
void CRUException::StoreData(CUOFsIpcMessageTranslator &translator) {
  int i;
  int numErrors = GetNumErrors();
  translator.WriteBlock(&numErrors, sizeof(int));
  for (i = 0; i < numErrors; i++) {
    int errorCode = GetErrorCode(i);
    translator.WriteBlock(&errorCode, sizeof(int));

    // Load the resource, substitute the arguments etc.
    BuildErrorMsg(i);
    // Only now we know the exact (null-terminated) buffer length
    int bufsize = GetErrorMsgLen(i);

    char *buffer = new char[bufsize];

    GetErrorMsg(i, buffer, bufsize);
    int strSize = strlen(buffer) + 1;  // Can be smaller than bufsize

    translator.WriteBlock(&strSize, sizeof(int));
    translator.WriteBlock(buffer, strSize);

    delete[] buffer;

    // Oblivious. The error message is already built.
    // StoreErrorParams(translator,i);
  }
}

//--------------------------------------------------------------------------//
//	CRUException::StoreErrorParams()
//--------------------------------------------------------------------------//
void CRUException::StoreErrorParams(CUOFsIpcMessageTranslator &translator, int index) {
  int i;

  int numLongParams = GetNumLongArguments(index);
  translator.WriteBlock(&numLongParams, sizeof(int));
  for (i = 0; i < numLongParams; i++) {
    int errorCode = GetLongArgument(index, i);
    translator.WriteBlock(&errorCode, sizeof(int));
  }

  int numStrParams = GetNumStrArguments(index);
  translator.WriteBlock(&numStrParams, sizeof(int));
  for (i = 0; i < numStrParams; i++) {
    const char *param = GetStrArgument(index, i);
    int strSize = strlen(param) + 1;
    translator.WriteBlock(&strSize, sizeof(int));
    translator.WriteBlock(param, strSize);
  }
}

//--------------------------------------------------------------------------//
//	CRUException::LoadData()
//--------------------------------------------------------------------------//
void CRUException::LoadData(CUOFsIpcMessageTranslator &translator) {
  int i;
  int numErrors;
  translator.ReadBlock(&numErrors, sizeof(int));
  for (i = 0; i < numErrors; i++) {
    int errorCode;
    translator.ReadBlock(&errorCode, sizeof(int));

    int strSize;
    translator.ReadBlock(&strSize, sizeof(int));

    char *buffer = new char[strSize];
    translator.ReadBlock(buffer, strSize);
    SetError(errorCode, buffer);

    delete[] buffer;

    // Oblivious. The error message was built by StoreData().
    // LoadErrorParams(translator,i);
  }
}

//--------------------------------------------------------------------------//
//	CRUException::LoadErrorParams()
//--------------------------------------------------------------------------//
void CRUException::LoadErrorParams(CUOFsIpcMessageTranslator &translator, int index) {
  int i;

  int numLongParams;
  translator.ReadBlock(&numLongParams, sizeof(int));
  for (i = 0; i < numLongParams; i++) {
    int errorCode;
    translator.ReadBlock(&errorCode, sizeof(int));
    AddArgument(errorCode);
  }

  int numStrParams;
  translator.ReadBlock(&numStrParams, sizeof(int));
  for (i = 0; i < numStrParams; i++) {
    int strSize;
    translator.ReadBlock(&strSize, sizeof(int));

    char *buffer = new char[strSize];
    translator.ReadBlock(buffer, strSize);
    AddArgument(buffer);

    delete[] buffer;
  }
}
