
#ifndef ELEMDDLUDRMAXRESULTS_H
#define ELEMDDLUDRMAXRESULTS_H
/* -*-C++-*-
******************************************************************************
*
* File:         ElemDDLUdrMaxResults.h
* Description:  class for UDR Max Results (parse node) elements in
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

class ElemDDLUdrMaxResults : public ElemDDLNode {
 public:
  // default constructor
  ElemDDLUdrMaxResults(ComUInt32 theMaxResults);

  // virtual destructor
  virtual ~ElemDDLUdrMaxResults(void);

  // cast
  virtual ElemDDLUdrMaxResults *castToElemDDLUdrMaxResults(void);

  // accessor
  inline const ComUInt32 getMaxResults(void) const { return maxResults_; }

  //
  // methods for tracing
  //

  virtual NATraceList getDetailInfo() const;
  virtual const NAString getText() const;

 private:
  ComUInt32 maxResults_;

};  // class ElemDDLUdrMaxResults

#endif /* ELEMDDLUDRMAXRESULTS_H */
