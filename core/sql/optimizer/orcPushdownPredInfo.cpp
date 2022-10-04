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
**********************************************************************/

#include "orcPushdownPredInfo.h"
#include "optimizer/ItemExpr.h"
#include "optimizer/ItemColRef.h"
#include "exp/exp_clause_derived.h"

void ExtPushdownPredInfo::display() {
  NAString text = getText();
  fprintf(stdout, "%s", text.data());
}

NAString ExtPushdownPredInfo::getText() {
  NAString result;
  NAString op;
  NABoolean doBinaryOp = FALSE;
  NABoolean doUnaryOp = FALSE;
  NABoolean doListOp = FALSE;
  switch (type_) {
    case UNKNOWN_OPER:
      return "UNKNOWN";
      break;
    case STARTAND:
      result += "and(";
      break;
    case STARTOR:
      result += "or(";
      break;
    case STARTNOT:
      result += "not(";
      break;
    case END:
      result += ")";
      break;
    case EQUALS:
      op = " = ";
      doBinaryOp = TRUE;
      break;
    case LESSTHAN:
      op = " < ";
      doBinaryOp = TRUE;
      break;
    case LESSTHANEQUALS:
      op = " <= ";
      doBinaryOp = TRUE;
      break;
    case ISNULL:
      op = " is null";
      doUnaryOp = TRUE;
      break;
    case IN:
      if (operValIdList().entries() > 0) {
        op = " in(";
        doListOp = TRUE;
      } else {
        op = " in ";
        doBinaryOp = TRUE;
      }
      break;
      //      case BETWEEN:
      //        return "BETWEEN";
      //        break;
    default:
      break;
  }

  if (doBinaryOp) {
    NAString x;
    colValId().getItemExpr()->unparse(x);
    result += x;
    result += op;
    x.remove(0);
    operValId().getItemExpr()->unparse(x);
    result += x;
  } else if (doUnaryOp) {
    NAString x;
    colValId().getItemExpr()->unparse(x);
    result += x;
    result += op;
  } else if (doListOp) {
    NAString x;
    colValId().getItemExpr()->unparse(x);
    result += x;
    result += op;

    x.remove(0);
    operValIdList().unparse(x);
    result += x;
    result += ")";
  }
  return result;
}

Int16 ExtPushdownPredInfo::extractFilterId(const ValueId &operValId) {
  Int16 filterId = -1;

  ItemExpr *expr = operValId.getItemExpr();

  if (expr && expr->getOperatorType() == ITM_HOSTVAR) {
    HostVar *x = (HostVar *)expr;
    if (x->isSystemGeneratedRangeOfValuesHV()) {
      filterId = x->extractFilterId();
    }
  }

  return filterId;
}

NAString ExtPushdownPredInfoList::getText() {
  NAString text;
  for (Int32 i = 0; i < entries(); i++) {
    text += (*this)[i].getText();
    text += " ";
  }
  return text;
}

void ExtPushdownPredInfoList::display() {
  NAString text = getText();
  fprintf(stdout, "%s\n", text.data());
}

void ExtPushdownPredInfoList::insertStartAND() { append(ExtPushdownPredInfo(STARTAND)); }

void ExtPushdownPredInfoList::insertStartOR() { append(ExtPushdownPredInfo(STARTOR)); }

void ExtPushdownPredInfoList::insertStartNOT() { append(ExtPushdownPredInfo(STARTNOT)); }

void ExtPushdownPredInfoList::insertEND() { append(ExtPushdownPredInfo(END)); }

void ExtPushdownPredInfoList::insertEQ(const ValueId &col, const ValueId &val) {
  append(ExtPushdownPredInfo(EQUALS, col, val));
}

void ExtPushdownPredInfoList::insertLESS(const ValueId &col, const ValueId &val) {
  append(ExtPushdownPredInfo(LESSTHAN, col, val));
}

void ExtPushdownPredInfoList::insertLESS_EQ(const ValueId &col, const ValueId &val) {
  append(ExtPushdownPredInfo(LESSTHANEQUALS, col, val));
}

void ExtPushdownPredInfoList::insertIS_NULL(const ValueId &col) { append(ExtPushdownPredInfo(ISNULL, col)); }

void ExtPushdownPredInfoList::insertIN(const ValueId &col, const ValueIdList &val) {
  append(ExtPushdownPredInfo(IN, col, val));
}

void ExtPushdownPredInfoList::insertIN(const ValueId &col, const ValueId &val) {
  append(ExtPushdownPredInfo(IN, col, val));
}

// returns: TRUE, if cannot be pushed down.
NABoolean ExtPushdownPredInfoList::validatePushdownForParquet() {
  // For parquet tables,
  // if pushdown operations are not supported on certain datatypes,
  // clear extListOfPPI_.
  // Currently it is not supported for CHAR, BYTE, DECIMAL, and
  // legacy TIMESTAMP types.
  // See TrafParquetFileReader.java::extractFilterInfo for details.
  NABoolean cantPushdown = FALSE;
  for (Lng32 i = 0; ((i < entries()) && (NOT cantPushdown)); i++) {
    ExtPushdownPredInfo &ppi = (*this)[i];
    ExtPushdownOperatorType type = ppi.getType();

    if (NOT((ppi.getType() == EQUALS) || (ppi.getType() == LESSTHAN) || (ppi.getType() == LESSTHANEQUALS) ||
            (ppi.getType() == ISNULL)))
      continue;

    ValueId colValId = ppi.colValId();
    if ((colValId != NULL_VALUE_ID) &&
        (((colValId.getType().getHiveType() == HIVE_DECIMAL_TYPE) && (ppi.getType() != ISNULL)) ||
         (colValId.getType().getHiveType() == HIVE_DATE_TYPE))) {
      cantPushdown = TRUE;
      continue;
    }

    if (((ppi.getType() == EQUALS) || (ppi.getType() == LESSTHAN) || (ppi.getType() == LESSTHANEQUALS)) &&
        (ppi.operValId().getType().errorsCanOccur(colValId.getType()))) {
      if (ppi.operValId().getItemExpr()->getOperatorType() == ITM_CONSTANT) {
        ConstValue *cv = (ConstValue *)ppi.operValId().getItemExpr();

        char convBuf[1000];
        short retCode =
            convDoIt((char *)cv->getConstValue(), cv->getStorageSize(), (short)cv->getType()->getFSDatatype(),
                     cv->getType()->getPrecision(), cv->getType()->getScale(), convBuf,
                     colValId.getType().getNominalSize(), colValId.getType().getFSDatatype(),
                     colValId.getType().getPrecision(), colValId.getType().getScale(), NULL, 0);
        if (retCode != ex_expr::EXPR_OK) cantPushdown = TRUE;
      } else
        cantPushdown = TRUE;
      continue;
    }
  }  // for

  return cantPushdown;
}

NABoolean ExtPushdownPredInfoList::validateOperatorsForParquetCppReader() {
  for (Lng32 i = 0; i < entries(); i++) {
    ExtPushdownPredInfo &ppi = (*this)[i];
    ExtPushdownOperatorType type = ppi.getType();

    if (ppi.getType() == STARTOR ||
        // can not handle real list yet
        (ppi.getType() == IN && ppi.operValIdList().entries() > 0))
      return FALSE;
  }

  return TRUE;
}

// returns: TRUE, if cannot be pushed down.
NABoolean ExtPushdownPredInfoList::validatePushdownForOrc() {
  // For orc tables,
  // if pushdown operations are not supported on certain datatypes,
  // return TRUE.
  // Currently ISNULL is not supported for date and timestamp due to a
  // bug in hive.
  // See mantis 3340 for details.
  NABoolean cantPushdown = FALSE;
  for (Lng32 i = 0; ((i < entries()) && (NOT cantPushdown)); i++) {
    ExtPushdownPredInfo &ppi = (*this)[i];
    ExtPushdownOperatorType type = ppi.getType();

    ValueId colValId = ppi.colValId();
    if ((ppi.getType() == ISNULL) &&
        ((colValId != NULL_VALUE_ID) && ((colValId.getType().getHiveType() == HIVE_DATE_TYPE) ||
                                         (colValId.getType().getHiveType() == HIVE_TIMESTAMP_TYPE)))) {
      cantPushdown = TRUE;
      continue;
    }

    if (((ppi.getType() == EQUALS) || (ppi.getType() == LESSTHAN) || (ppi.getType() == LESSTHANEQUALS)) &&
        (ppi.operValId().getType().errorsCanOccur(colValId.getType()))) {
      if (ppi.operValId().getItemExpr()->getOperatorType() == ITM_CONSTANT) {
        ConstValue *cv = (ConstValue *)ppi.operValId().getItemExpr();

        char convBuf[1000];
        short retCode =
            convDoIt((char *)cv->getConstValue(), cv->getStorageSize(), (short)cv->getType()->getFSDatatype(),
                     cv->getType()->getPrecision(), cv->getType()->getScale(), convBuf,
                     colValId.getType().getNominalSize(), colValId.getType().getFSDatatype(),
                     colValId.getType().getPrecision(), colValId.getType().getScale(), NULL, 0);
        if (retCode != ex_expr::EXPR_OK) cantPushdown = TRUE;
      } else
        cantPushdown = TRUE;
      continue;
    }
  }  // for

  return cantPushdown;
}
