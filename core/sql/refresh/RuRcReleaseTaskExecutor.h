
#ifndef _RU_RC_RELEASE_TASKEX_H_
#define _RU_RC_RELEASE_TASKEX_H_

/* -*-C++-*-
******************************************************************************
*
* File:         RuRcReleaseTaskExecutor.cpp
* Description:  Implementation of class	CRURcReleaseTaskExecutor
*
* Created:      10/18/2000
* Language:     C++
*
*
******************************************************************************
*/

#include "refresh.h"
#include "RuTaskExecutor.h"

class REFRESH_LIB_CLASS CRURcReleaseTaskExecutor : public CRUTaskExecutor {
 private:
  typedef CRUTaskExecutor inherited;

 public:
  CRURcReleaseTaskExecutor(CRUTask *pParentTask);
  ~CRURcReleaseTaskExecutor() {}

 public:
  //-- Implementation of pure virtual functions
  virtual void Work();

  virtual void Init();

 public:
  // These functions serialize/de-serialize the executor's context
  // for the message communication with the remote server process

  // Used in the main process side
  virtual void StoreRequest(CUOFsIpcMessageTranslator &translator){};
  virtual void LoadReply(CUOFsIpcMessageTranslator &translator){};

  // Used in the remote process side
  virtual void LoadRequest(CUOFsIpcMessageTranslator &translator){};
  virtual void StoreReply(CUOFsIpcMessageTranslator &translator){};

 protected:
  //-- Implementation of pure virtual

  virtual int GetIpcBufferSize() const {
    return 0;  // The task is always performed locally
  }

 private:
  //-- Prevent copying
  CRURcReleaseTaskExecutor(const CRURcReleaseTaskExecutor &other);
  CRURcReleaseTaskExecutor &operator=(const CRURcReleaseTaskExecutor &other);
};

#endif
