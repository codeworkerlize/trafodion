
#ifndef MXCOMPILEUSERMODULE__H
#define MXCOMPILEUSERMODULE__H

/* -*-C++-*-
 *****************************************************************************
 * File:         mxCompileUserModule.h
 * Description:  This class holds the heap, diags & other global variables used
 *               by the mxCompileUserModule tool.
 * Created:      05/19/2003
 * Language:     C++
 *****************************************************************************
 */

#include "mxcmpReturnCodes.h"

class ComDiagsArea;
class DgBase;
class NAHeap;

class mxCompileUserModule {
 public:
  mxCompileUserModule();
  virtual ~mxCompileUserModule();

  ComDiagsArea &operator<<(const DgBase &);
  void internalError(const char *file, int line, const char *msg);

  void dumpDiags();
  int diagsCount();

  ComDiagsArea &operator<<(mxcmpExitCode rc) {
    setReturnCode(rc);
    return *diags_;
  }
  void setReturnCode(mxcmpExitCode rc);
  mxcmpExitCode returnCode() { return returnCode_; }

  NAHeap *heap();

 private:
  NAHeap *heap_;
  ComDiagsArea *diags_;
  mxcmpExitCode returnCode_;
};

inline NAHeap *mxCompileUserModule::heap() { return heap_; }

extern mxCompileUserModule *mxCUMptr;

#define mxCUMinternalError(msg) mxCUMptr->internalError(__FILE__, __LINE__, msg)

#endif  // MXCOMPILEUSERMODULE__H
