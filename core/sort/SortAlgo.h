
#ifndef SORTALGO_H
#define SORTALGO_H

/* -*-C++-*-
******************************************************************************
*
* File:         SortAlgo.h
* RCS:          $Id: SortAlgo.h,v 1.2.16.2 1998/05/26 22:32:37  Exp $
*
* Description:  This class represents the generic SortAlgorithm base class.
*               Specific  algorithms are implemented as subclasses of this
*               class. Note that SortAlgorithm is a virtual base class since
*               it contains pure virtual functions.
*
* Created:	    05/22/96
* Modified:     $ $Date: 1998/05/26 22:32:37 $ (GMT)
* Language:     C++
* Status:       $State: Exp $
*
******************************************************************************
*/

#include <fstream>

#include "CommonStructs.h"
#include "Const.h"
#include "ScratchSpace.h"
#include "common/str.h"
#include "export/NABasicObject.h"

class SortAlgo : public NABasicObject {
 public:
  SortAlgo(int runsize, int recsize, NABoolean doNotallocRec, int keysize, SortScratchSpace *scratch, int explainNodeId,
           ExBMOStats *bmoStats);
  ~SortAlgo(){};

  //------------------------------------------------------------
  // Note that sort is implemented as a pure virtual function.
  //------------------------------------------------------------

  virtual int sortSend(void *rec, int len, void *tupp) = 0;

  virtual int sortClientOutOfMem(void) = 0;

  virtual int sortSendEnd(void) = 0;

  virtual int sortReceive(void *rec, int &len) = 0;
  virtual int sortReceive(void *&rec, int &len, void *&tupp) = 0;
  virtual UInt32 getOverheadPerRecord(void) = 0;
  //-----------------------------------------------------------
  // Since the compare routine is independent of the sort
  // algorithm it can be a member of this base class.
  //-----------------------------------------------------------
  short compare(char *key1, char *key2);
  virtual int generateInterRuns() = 0;
  int getNumOfCompares() const;
  SortScratchSpace *getScratch() const;
  int getRunSize() const;
  void setExternalSort(void) { internalSort_ = FALSE_L; }
  NABoolean isInternalSort(void) { return internalSort_; }

 protected:
  int numCompares_;
  int runSize_;
  int recSize_;
  int keySize_;
  SortScratchSpace *scratch_;
  NABoolean sendNotDone_;
  NABoolean internalSort_;
  NABoolean doNotallocRec_;
  int explainNodeId_;
  ExBMOStats *bmoStats_;
};

#endif
