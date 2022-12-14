
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         CliMsgObj.cpp
 * Description:  CLI Message Objects
 *
 * Created:      4/18/2001
 * Language:     C++
 *
 *
 *****************************************************************************
 */
#include "executor/CliMsgObj.h"

#include <iostream>

#include "common/ComplexObject.h"
#include "cli/sqlcli.h"
#include "common/ComTransInfo.h"
#include "export/IpcMessageObj.h"
// allocate memory from the heap and duplicate the string
static char *dupCharStar(NAMemory *heap, char *source) {
  char *target = NULL;
  if (source) {
    int length = str_len(source);
    if (heap) {
      target = (char *)heap->allocateMemory(length + 1);
    } else {
      target = ::new char[length + 1];
    }
    str_cpy_all(target, source, length + 1);
  }
  return target;
}

// return memory of the string to the heap
static void deallocateCharStar(NAMemory *heap, char *text) {
  if (text) {
    if (heap) {
      heap->deallocateMemory(text);
    } else {
      ::delete[] text;
    }
  }
}

// CtrlStmtComplexObject defintions
CtrlStmtComplexObject::CtrlStmtComplexObject(NAMemory *heap, char *sqlText, Int16 sqlTextCharSet,
                                             CtrlStmtComplexObject *ctrlObj)
    : ctrlObj_(ctrlObj), ComplexObject(heap, CtrlStmtComplexObjectType) {
  if (sqlText) {
    sqlText_ = dupCharStar(heap, sqlText);
    sqlTextCharSet_ = sqlTextCharSet;
  } else
    sqlTextCharSet_ = (Int16)SQLCHARSETCODE_UNKNOWN;
}

CtrlStmtComplexObject::CtrlStmtComplexObject(char *sqlText, Int16 sqlTextCharSet, CtrlStmtComplexObject *ctrlObj)
    : ctrlObj_(ctrlObj), ComplexObject(NULL, CtrlStmtComplexObjectType) {
  if (sqlText) {
    sqlText_ = dupCharStar(NULL, sqlText);
    sqlTextCharSet_ = sqlTextCharSet;
  } else
    sqlTextCharSet_ = (Int16)SQLCHARSETCODE_UNKNOWN;
}

CtrlStmtComplexObject::CtrlStmtComplexObject(NAMemory *heap)
    : sqlText_(NULL),
      sqlTextCharSet_((Int16)SQLCHARSETCODE_UNKNOWN),
      ctrlObj_(NULL),
      ComplexObject(heap, CtrlStmtComplexObjectType) {}

CtrlStmtComplexObject::CtrlStmtComplexObject()
    : sqlText_(NULL),
      sqlTextCharSet_((Int16)SQLCHARSETCODE_UNKNOWN),
      ctrlObj_(NULL),
      ComplexObject(NULL, CtrlStmtComplexObjectType) {}

CtrlStmtComplexObject::~CtrlStmtComplexObject() {
  NAMemory *heap = getHeap();
  deallocateCharStar(heap, sqlText_);
}

void CtrlStmtComplexObject::freeSubObjects() {
  if (ctrlObj_) {
    ctrlObj_->freeSubObjects();
    delete ctrlObj_;
  }
}

void CtrlStmtComplexObject::sharedOperationSequence(MessageOperator *msgOp, InputContainer *input,
                                                    OutputContainer *output) {
  baseOperation(msgOp, input, output);
  msgOp->setInputOutputForNextOperation(input, output);

  CharPtrObjectContainer sqlTextWrap(&sqlText_);
  msgOp->execute(&sqlTextWrap, input, output);
  msgOp->setInputOutputForNextOperation(input, output);

  ComplexObjectPtrContainer ctrlObjWrap((ComplexObjectPtr *)&ctrlObj_);
  msgOp->execute(&ctrlObjWrap, input, output);
}

char *CtrlStmtComplexObject::getSqlText() { return sqlText_; }

Int16 CtrlStmtComplexObject::getSqlTextCharSet() { return sqlTextCharSet_; }

CtrlStmtComplexObject *CtrlStmtComplexObject::getCtrlStmt() { return ctrlObj_; }

void CtrlStmtComplexObject::dump() {
  if (sqlText_) cout << sqlText_ << endl;
  if (ctrlObj_) {
    ctrlObj_->dump();
  }
}

// TransAttr definitions

TransAttrComplexObject::TransAttrComplexObject(NAMemory *heap, TransMode::AccessMode mode,
                                               TransMode::IsolationLevel isoLv, int diagSize,
                                               TransMode::RollbackMode rollbackMode, int autoabortInterval)
    : mode_(mode),
      isoLv_(isoLv),
      diagSize_(diagSize),
      rollbackMode_(rollbackMode),
      autoabortInterval_(autoabortInterval),
      ComplexObject(heap, TransAttrComplexObjectType) {}

TransAttrComplexObject::TransAttrComplexObject(TransMode::AccessMode mode, TransMode::IsolationLevel isoLv,
                                               int diagSize, TransMode::RollbackMode rollbackMode,
                                               int autoabortInterval)
    : mode_(mode),
      isoLv_(isoLv),
      diagSize_(diagSize),
      rollbackMode_(rollbackMode),
      autoabortInterval_(autoabortInterval),
      ComplexObject(NULL, TransAttrComplexObjectType) {}

TransAttrComplexObject::TransAttrComplexObject(NAMemory *heap)
    : mode_(TransMode::AM_NOT_SPECIFIED_),
      isoLv_(TransMode::IL_NOT_SPECIFIED_),
      diagSize_(-1),
      rollbackMode_(TransMode::ROLLBACK_MODE_NOT_SPECIFIED_),
      autoabortInterval_(-1),
      ComplexObject(heap, TransAttrComplexObjectType) {}

TransAttrComplexObject::TransAttrComplexObject()
    : mode_(TransMode::AM_NOT_SPECIFIED_),
      isoLv_(TransMode::IL_NOT_SPECIFIED_),
      diagSize_(-1),
      rollbackMode_(TransMode::ROLLBACK_MODE_NOT_SPECIFIED_),
      autoabortInterval_(-1),
      ComplexObject(NULL, TransAttrComplexObjectType) {}

TransAttrComplexObject::~TransAttrComplexObject() {}

void TransAttrComplexObject::freeSubObjects() {}

void TransAttrComplexObject::sharedOperationSequence(MessageOperator *msgOp, InputContainer *input,
                                                     OutputContainer *output) {
  baseOperation(msgOp, input, output);
  msgOp->setInputOutputForNextOperation(input, output);

  EnumObjectContainer modeWrap((enum ComplexObjectType *)&mode_);
  msgOp->execute(&modeWrap, input, output);
  msgOp->setInputOutputForNextOperation(input, output);

  EnumObjectContainer isoLvWrap((enum ComplexObjectType *)&isoLv_);
  msgOp->execute(&isoLvWrap, input, output);
  msgOp->setInputOutputForNextOperation(input, output);

  LongObjectContainer diagSizeWrap(&diagSize_);
  msgOp->execute(&diagSizeWrap, input, output);
  msgOp->setInputOutputForNextOperation(input, output);

  EnumObjectContainer rbModeWrap((enum ComplexObjectType *)&rollbackMode_);
  msgOp->execute(&rbModeWrap, input, output);
  msgOp->setInputOutputForNextOperation(input, output);

  LongObjectContainer autoabortIntervalWrap(&autoabortInterval_);
  msgOp->execute(&autoabortIntervalWrap, input, output);
}

TransMode::AccessMode TransAttrComplexObject::getAccessMode() { return mode_; }

TransMode::IsolationLevel TransAttrComplexObject::getIsolationLevel() { return isoLv_; }

TransMode::RollbackMode TransAttrComplexObject::getRollbackMode() { return rollbackMode_; }

int TransAttrComplexObject::getDiagSize() { return diagSize_; }

int TransAttrComplexObject::getAutoabortInterval() { return autoabortInterval_; }

void TransAttrComplexObject::setAccessMode(TransMode::AccessMode mode) { mode_ = mode; }

void TransAttrComplexObject::setRollbackMode(TransMode::RollbackMode rollbackMode) { rollbackMode_ = rollbackMode; }

void TransAttrComplexObject::setIsolationLevel(TransMode::IsolationLevel isoLv) { isoLv_ = isoLv; }

void TransAttrComplexObject::setDiagSize(int diagSize) { diagSize_ = diagSize; }

void TransAttrComplexObject::setAutoabortInterval(int autoabortInterval) { autoabortInterval_ = autoabortInterval; }

void TransAttrComplexObject::dump() {
  cout << "TRANSACTION ";
  const char *strOfIsoLv = getStrOfIsolationLevel(isoLv_);
  if (strOfIsoLv) {
    cout << "ISOLATION LEVEL " << strOfIsoLv << " ";
  }
  const char *strOfMode = getStrOfAccessMode(mode_);
  if (strOfMode) {
    cout << strOfMode << " ";
  }
  const char *strOfRbMode = getStrOfRollbackMode(rollbackMode_);
  if (strOfRbMode) {
    cout << strOfRbMode << " ";
  }
  if (diagSize_ > 0) {
    cout << "Diagnostic Size" << diagSize_;
  }
  if (autoabortInterval_ != -1) {
    cout << "Autoabort Interval" << autoabortInterval_;
  }
}

ComplexObject *CliComplexObjectFactory::manufacture(NAMemory *heap, ComplexObjectType objType) {
  switch (objType) {
    case CtrlStmtComplexObjectType:
      return new (heap) CtrlStmtComplexObject(heap);
      break;
    case TransAttrComplexObjectType:
      return new (heap) TransAttrComplexObject(heap);
      break;
    default:
      return NULL;
  }
}
