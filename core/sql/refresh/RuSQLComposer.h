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
//
**********************************************************************/
#ifndef _RU_SQL_COMPOSER_H_
#define _RU_SQL_COMPOSER_H_

/* -*-C++-*-
******************************************************************************
*
* File:         RuSQLComposer.h
* Description:  Definition of class CRUSQLComposer
*
*
* Created:      08/13/2000
* Language:     C++
*
*
*
******************************************************************************
*/

//--------------------------------------------------------------------------//
//	CRUSQLComposer
//
//	This is an abstract class for all sql composers classes.
//	Each Composer may have its own compose functions. After composing a
//	statement the user may retrieve the sql text by calling GetSql().
//
//	The class also implements some service functions (like transalating
//	numbers into strings, using special sql syntax etc.).
//
//--------------------------------------------------------------------------//

#include "refresh.h"
#include "dsstring.h"

#include "RuException.h"

class CRUTask;

class REFRESH_LIB_CLASS CRUSQLComposer {
  //----------------------------------//
  //	Public Members
  //----------------------------------//
 public:
  CRUSQLComposer() : sql_("") {}
  virtual ~CRUSQLComposer() {}

 public:
  // Retrieve the text generated by the last call to ComposeXXX()
  const CDSString &GetSQL() const { return sql_; }

  static CDSString TInt32ToStr(TInt32 num);
  static CDSString TInt64ToStr(TInt64 src);
  static CDSString TInt64ToTwoStrs(TInt64 num);

  // Return a string that represent a cast expression for execution param
  static CDSString ComposeCastExpr(const CDSString &type);

  // Add a prefix/suffix to a (possibly quoted) identifier string.
  // Take care to leave the quotes in place, if there are ones
  static void AddPrefixToString(const CDSString &prefix, CDSString &to);
  static void AddSuffixToString(const CDSString &suffix, CDSString &to);

  // Returns the name as a quoted name with the given prefix
  static CDSString ComposeQuotedColName(const CDSString &prefix, const CDSString &name);

  //----------------------------------//
  //	Protected Members
  //----------------------------------//
 protected:
  // Text generated by the last call to ComposeXXX()
  // A temporary buffer to hold the generated SQL
  // Since this data member is used VERY often, it is defined as protected.
  CDSString sql_;

  //----------------------------------//
  //	Private Members
  //----------------------------------//
 private:
  //-- Prevent copying
  CRUSQLComposer(const CRUSQLComposer &other);
  CRUSQLComposer &operator=(const CRUSQLComposer &other);
};

//--------------------------------------------------------------------------//
//	The inliners
//--------------------------------------------------------------------------//

inline CDSString CRUSQLComposer::TInt32ToStr(TInt32 num) {
  char buf[20];
  sprintf(buf, "%d", num);
  return CDSString(buf);
}

//--------------------------------------------------------------------------//

inline CDSString CRUSQLComposer::TInt64ToStr(TInt64 src) {
  const int maxBufferSize = 50;

  TInt64 temp = src;  // (src >= 0) ? src : - src;
  char buffer[maxBufferSize];
  char *s = &buffer[maxBufferSize];
  *--s = '\0';
  do {
#ifdef NA_NSK
    int c = (int)(temp % 10);
#else
    char c = (char)(temp % 10);
#endif  // NA_NSK
    if (c < 0) {
      c = -c;
    }
    *--s = (char)(c + '0');
    temp /= 10;
  } while (temp != 0);
  if (src < 0) {
    *--s = '-';
  }
  return CDSString(s);
}

//--------------------------------------------------------------------------//

inline CDSString CRUSQLComposer::TInt64ToTwoStrs(TInt64 num) {
  const int maxBufferSize = 50;
  char buf[maxBufferSize];

  // Break a quad-word into two double-words
  TInt32 msw, lsw;
  msw = (TInt32)(num >> 32);
  lsw = (TInt32)(num - ((TInt64)msw << 32));

  sprintf(buf, "%d %d", msw, lsw);

  return CDSString(buf);
}

//--------------------------------------------------------------------------//

inline CDSString CRUSQLComposer::ComposeCastExpr(const CDSString &type) { return "CAST (? AS " + type + ")"; }

//--------------------------------------------------------------------------//

inline CDSString CRUSQLComposer::ComposeQuotedColName(const CDSString &prefix, const CDSString &name) {
  CDSString rtn(name);

  AddPrefixToString(prefix, rtn);

  if ('"' != rtn.operator[](0)) {
    // The string is not quoted yet...
    rtn = "\"" + rtn + "\"";
  }

  return rtn;
}

//--------------------------------------------------------------------------//

inline void CRUSQLComposer::AddPrefixToString(const CDSString &prefix, CDSString &to) {
  char firstchar = to.operator[](0);
  if ('"' == firstchar) {
    // A delimited identifier. Remove the leading '"'.
    to.Remove(0, 1);
    // And complement it at the beginning
    to = "\"" + prefix + to;
  } else {
    to = prefix + to;
  }
}

//--------------------------------------------------------------------------//

inline void CRUSQLComposer::AddSuffixToString(const CDSString &suffix, CDSString &to) {
  int len = to.GetLength();
  RUASSERT(len > 0);

  char lastchar = to.operator[](len - 1);
  if ('"' == lastchar) {
    // A delimited identifier. Remove the trailing '"'.
    to.Remove(len - 1, 1);
    // And complement it at the end
    to += suffix + "\"";
  } else {
    to += suffix;
  }
}

#endif
