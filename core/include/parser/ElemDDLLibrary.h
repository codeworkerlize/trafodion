//******************************************************************************

//******************************************************************************
#ifndef ELEMDDLUDRLIBRARY_H
#define ELEMDDLUDRLIBRARY_H
/* -*-C++-*-
********************************************************************************
*
* File:         ElemDDLUdrLibrary.h
* Description:  class for UDR Library (parse node) elements in
*               DDL statements
*
*
* Created:      10/14/2011
* Language:     C++
*
*
*
*
********************************************************************************
*/

#include "parser/ElemDDLNode.h"
#include "common/ComSmallDefs.h"

class ElemDDLLibrary : public ElemDDLNode {
 public:
  ElemDDLUdrLibrary(NAString &theLibrary);

  virtual ~ElemDDLUdrLibrary();

  virtual ElemDDLUdrLibrary *castToElemDDLUdrLibrary(void);

  inline const NAString &getlibraryName(void) const { return libraryName_; }

  //
  // methods for tracing
  //

  virtual const NAString displayLabel1() const;
  virtual NATraceList getDetailInfo() const;
  virtual const NAString getText() const;

 private:
  NAString &libraryName_;
};  // class ElemDDLUdrLibrary

#endif /* ELEMDDLUDRLIBRARY_H */
