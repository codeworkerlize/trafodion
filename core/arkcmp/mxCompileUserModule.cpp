
/* -*-C++-*-
 *****************************************************************************
 * File:         mxCompileUserModule.cpp
 * Description:  This is the main class for SQL compiling a C/C++/Cobol
 *               executable/library or SQLJ ser/jar file that has embedded
 *               module definitions.
 * Created:      03/03/2003
 * Language:     C++
 *****************************************************************************
 */

#include "mxCompileUserModule.h"

#include <iostream>

#include "common/DgBaseType.h"
#include "common/NAMemory.h"
#include "common/Platform.h"
#include "export/ComDiags.h"
#include "sqlmsg/ErrorMessage.h"

mxCompileUserModule::mxCompileUserModule() : heap_(NULL), diags_(NULL), returnCode_(SUCCEED) {
  heap_ = new NAHeap("mxCompileUserModule Heap", NAMemory::DERIVED_FROM_SYS_HEAP, (int)524288);

  diags_ = ComDiagsArea::allocate(heap_);
}

mxCompileUserModule::~mxCompileUserModule() {
  if (diags_) {
    diags_->decrRefCount();
  }
  if (heap_) {
    delete heap_;
    heap_ = NULL;
  }
}

ComDiagsArea &mxCompileUserModule::operator<<(const DgBase &dgObj) {
  if (!diags_) {
    cerr << "Error: ComDiagsArea is not yet created." << endl;
    exit(1);
  }
  return *diags_ << dgObj;
}

void mxCompileUserModule::dumpDiags() {
  if (diagsCount()) {
    NADumpDiags(cerr, diags_, TRUE);
    diags_->clear();
  }
}

int mxCompileUserModule::diagsCount() {
  return !diags_ ? 0 : (diags_->getNumber(DgSqlCode::ERROR_) + diags_->getNumber(DgSqlCode::WARNING_));
}

void mxCompileUserModule::internalError(const char *file, int line, const char *msg) {
  *this << DgSqlCode(-2214) << DgString0(file) << DgInt0(line) << DgString1(msg);
}

void mxCompileUserModule::setReturnCode(mxcmpExitCode rc) {
  switch (returnCode_) {
    case FAIL:
      break;  // always report FAIL
    case ERROR:
      if (rc == FAIL) returnCode_ = rc;  // report FAIL over ERROR
      break;
    case WARNING:
      if (rc == FAIL || rc == ERROR) returnCode_ = rc;  // report FAIL, ERROR over WARNING
      break;
    default:
      returnCode_ = rc;  // report FAIL, ERROR, WARNING over SUCCEED
      break;
  }
}
