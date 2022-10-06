
/* -*-C++-*-
******************************************************************************
*
* File:         GenUdr.cpp
* Description:  Generator code for Fast transport
* Created:      11/05/2012
* Language:     C++
*
******************************************************************************
*/

#include "optimizer/RelMisc.h"
//#include "LmGenUtil.h"
#include "GenExpGenerator.h"
#include "LmError.h"
#include "comexe/ComQueue.h"
#include "comexe/ExplainTuple.h"
#include "comexe/ExplainTupleMaster.h"
#include "executor/sql_buffer.h"
#include "generator/Generator.h"
//#include "optimizer/UdfDllInteraction.h"
#include "RelFastTransport.h"

// Helper function to allocate a string in the plan
static char *AllocStringInSpace(ComSpace &space, const char *s) {
  char *result = space.allocateAndCopyToAlignedSpace(s, str_len(s));
  return result;
}

// Helper function to allocate binary data in the plan. The data
// will be preceded by a 4-byte length field
static char *AllocDataInSpace(ComSpace &space, const char *data, UInt32 len) {
  char *result = space.allocateAndCopyToAlignedSpace(data, len, 4);
  return result;
}

//
// Helper function to get the maximum number of characters required
// to represent a value of a given NAType.
//
static int GetDisplayLength(const NAType &t) {
  int result = t.getDisplayLength(t.getFSDatatype(), t.getNominalSize(), t.getPrecision(), t.getScale(), 0);
  return result;
}

// Helper function to create an ItemExpr tree from a scalar
// expression string.
static ItemExpr *ParseExpr(NAString &s, CmpContext &c, ItemExpr &ie) {
  ItemExpr *result = NULL;
  Parser parser(&c);
  result = parser.getItemExprTree(s.data(), s.length(), CharInfo::UTF8, 1, &ie);
  return result;
}

//
// Helper function to create an ItemExpr tree that converts
// a SQL value, represented by the source ItemExpr, to the
// target type.
//
static ItemExpr *CreateCastExpr(ItemExpr &source, const NAType &target, CmpContext *cmpContext) {
  ItemExpr *result = NULL;
  NAMemory *h = cmpContext->statementHeap();

  NAString *s;
  s = new (h) NAString("cast(@A1 as ", h);

  (*s) += target.getTypeSQLname(TRUE);

  if (!target.supportsSQLnull()) (*s) += " NOT NULL";

  (*s) += ");";

  result = ParseExpr(*s, *cmpContext, source);

  return result;
}

int CreateAllCharsExpr(const NAType &formalType, ItemExpr &actualValue, CmpContext *cmpContext, ItemExpr *&newExpr) {
  int result = 0;
  NAMemory *h = cmpContext->statementHeap();
  NAType *typ = NULL;

  int maxLength = GetDisplayLength(formalType);
  maxLength = MAXOF(maxLength, 1);

  if (NOT DFS2REC::isCharacterString(formalType.getFSDatatype())) {
    typ = new (h) SQLVarChar(h, maxLength);
  } else {
    const CharType &cFormalType = (CharType &)formalType;
    CharInfo::CharSet charset;
    if (cFormalType.getCharSet() == CharInfo::CharSet::UCS2) {
      charset = CharInfo::CharSet::UTF8;
      // bytes per char is 2 for ucs2, while 4 for utf8, so the maxLength should be double
      maxLength = maxLength * 2;
    } else
      charset = cFormalType.getCharSet();
    typ =
        new (h) SQLVarChar(h, (maxLength == 0 ? 1 : maxLength), cFormalType.supportsSQLnull(),
                           cFormalType.isUpshifted(), cFormalType.isCaseinsensitive(), charset,
                           cFormalType.getCollation(), cFormalType.getCoercibility(), cFormalType.getEncodingCharSet());
  }

  newExpr = CreateCastExpr(actualValue, *typ, cmpContext);
  if (newExpr == NULL) {
    result = -1;
  }

  return result;
}
