
#ifndef ELEMDDLUDREXTERNALNAME_H
#define ELEMDDLUDREXTERNALNAME_H
/* -*-C++-*-
******************************************************************************
*
* File:         ElemDDLUdrExternalName.h
* Description:  class for UDR External Name (parse node) elements in
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

class ElemDDLUdrExternalName : public ElemDDLNode {
 public:
  // default constructor
  ElemDDLUdrExternalName(NAString &theName);

  // virtual destructor
  virtual ~ElemDDLUdrExternalName();

  // cast
  virtual ElemDDLUdrExternalName *castToElemDDLUdrExternalName(void);

  // accessor
  inline const NAString &getExternalName(void) const { return externalName_; }

  //
  // methods for tracing
  //

  virtual const NAString displayLabel1() const;
  virtual NATraceList getDetailInfo() const;
  virtual const NAString getText() const;

 private:
  NAString &externalName_;

};  // class ElemDDLUdrExternalName

#endif /* ELEMDDLUDREXTERNALNAME_H */
