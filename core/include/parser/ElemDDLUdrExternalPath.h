
#ifndef ELEMDDLUDREXTERNALPATH_H
#define ELEMDDLUDREXTERNALPATH_H
/* -*-C++-*-
******************************************************************************
*
* File:         ElemDDLUdrExternalPath.h
* Description:  class for UDR External Path (parse node) elements in
*               DDL statements
*
*
* Created:      10/08/1999
* Language:     C++
*
*
*
*
******************************************************************************
*/

#include "ElemDDLNode.h"
#include "common/ComSmallDefs.h"

class ElemDDLUdrExternalPath : public ElemDDLNode {
 public:
  // default constructor
  ElemDDLUdrExternalPath(NAString &thePath);

  // virtual destructor
  virtual ~ElemDDLUdrExternalPath();

  // cast
  virtual ElemDDLUdrExternalPath *castToElemDDLUdrExternalPath(void);

  // accessor
  inline const NAString &getExternalPath(void) const { return externalPath_; }

  //
  // methods for tracing
  //

  virtual const NAString displayLabel1() const;
  virtual NATraceList getDetailInfo() const;
  virtual const NAString getText() const;

 private:
  NAString &externalPath_;

};  // class ElemDDLUdrExternalPath

#endif /* ELEMDDLUDREXTERNALPATH_H */
