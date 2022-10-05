// **********************************************************************

// **********************************************************************

// ***********************************************************************
//
// File:         QRDescriptorStubs.cpp
//
// Description:  Stubs for QRDescriptor methods used only by MXCMP.
//               The full implementation of these methods is in
//               the optimizer folder.
//
// Created:      08/24/2010
// ***********************************************************************

#include "qmscommon/QRDescriptor.h"

#define THROW_EXCEPTION                                                 \
  assertLogAndThrow(CAT_QR_DESC_GEN, LL_ERROR, FALSE, QRLogicException, \
                    "Method toItemExpr() called from server process.");

ItemExpr *QRColumn::toItemExpr(const NAString &mvName, CollHeap *heap, TableNameScanHash *scanhash) {
  THROW_EXCEPTION;
  return NULL;
}
ItemExpr *QRMVColumn::toItemExpr(const NAString &mvName, CollHeap *heap, TableNameScanHash *scanhash) {
  THROW_EXCEPTION;
  return NULL;
}
ItemExpr *QRScalarValue::toItemExpr(const NAString &mvName, CollHeap *heap, TableNameScanHash *scanhash) {
  THROW_EXCEPTION;
  return NULL;
}
ItemExpr *QRNullVal::toItemExpr(const NAString &mvName, CollHeap *heap, TableNameScanHash *scanhash) {
  THROW_EXCEPTION;
  return NULL;
}
ItemExpr *QRBinaryOper::toItemExpr(const NAString &mvName, CollHeap *heap, TableNameScanHash *scanhash) {
  THROW_EXCEPTION;
  return NULL;
}
ItemExpr *QRUnaryOper::toItemExpr(const NAString &mvName, CollHeap *heap, TableNameScanHash *scanhash) {
  THROW_EXCEPTION;
  return NULL;
}
ItemExpr *QRFunction::toItemExpr(const NAString &mvName, CollHeap *heap, TableNameScanHash *scanhash) {
  THROW_EXCEPTION;
  return NULL;
}
