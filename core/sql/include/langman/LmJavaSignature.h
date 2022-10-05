
#ifndef LMJAVASIGNATURE_H
#define LMJAVASIGNATURE_H
/* -*-C++-*-
******************************************************************************
*
* File:         LmJavaSignature.h
* Description:  Java Signature
*
* Created:      06/17/2003
* Language:     C++
*
*
******************************************************************************
*/

#include "LmError.h"
#include "langman/LmCommon.h"
#include "common/ComSmallDefs.h"

//////////////////////////////////////////////////////////////////////
//
// Contents
//
//////////////////////////////////////////////////////////////////////
class LmJavaSignature;

//////////////////////////////////////////////////////////////////////
//
// LmJavaSignature: Represents a Java signature.
//   This class contains the encoded signature in Java encoded
//   format, decoded signature and the size of the decoded
//   signature text.
//
//   Example: For an SPJ with parameters
//            (int, java.sql.Date, java.sql.Time[])
//
//   encodedSignature_ = (ILjava/sql/Date;[Ljava/sql/Time;)V
//   unpackedSignature_ = (int,java.sql.Date,java.sql.Time[])
//   unpackedSignatureSize_ =  35
//
//  Interface methods:
//   createSig() : Generates encoded signature from SQL parameter types
//     This interface method is called by CATMAN code. Catman then
//     sends the generated signature to udrserver for validation.
//
//   unpackSignature() : decodes signature in Java format
//     to human readable format. Callers need to call
//     getUnpackedSignatureSize() and allocate those many bytes
//     before calling this method.
//
//   getUnpackedSignatureSize(): Calculates the size of the decoded
//     signature text.
//
//  Note: This Class is used by LM, CATMAN, SQLCOMP and DDOL. DDOL
//  wants only the ability to unpack the signature. And DDOL does not
//  include /common directory, so we need to #ifndef DDOL_IGNORE
//  for the code where /common is used.
//
//  This file is directly compiled into individual libraries. So this
//  class is not exported from langman.dll.
//
//////////////////////////////////////////////////////////////////////
class LmJavaSignature {
 public:
  LmJavaSignature(const char *encodedSignature = NULL, void *heap = NULL);

  ~LmJavaSignature();

  LmResult createSig(ComFSDataType paramType[], ComUInt32 paramSubType[], ComColumnDirection direction[],
                     ComUInt32 numParam, ComFSDataType resultType, ComUInt32 resultSubType, ComUInt32 numResultSets,
                     const char *optionalSig, ComBoolean isUdrForJavaMain, char *sigBuf, ComUInt32 sigLen,
                     ComDiagsArea *da);

  int unpackSignature(char *unpackedSignature);

  int getUnpackedSignatureSize();

  // Returns the total number (SQL + Result set) of parameters
  // present in the method signature
  int getParamCount() const { return numParams_; }

 private:
  NAMemory *heap_;
  char *encodedSignature_;
  int unpackedSignatureSize_;
  int numParams_;

  void setUnpackedSignatureSize(int size) { unpackedSignatureSize_ = size; }
};

#endif
