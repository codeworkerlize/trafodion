
#ifndef ELEMDDLUDRSURROGATELOCATION_H
#define ELEMDDLUDRSURROGATELOCATION_H
/* -*-C++-*-
******************************************************************************
*
* File:         ElemDDLUdrExternalPath.h
* Description:  class for UDR Surrogate Location (parse node) elements in
*               DDL statements
*
*
* Created:      07/28/2000
* Language:     C++
*
*
*
*
******************************************************************************
*/

#include "parser/ElemDDLNode.h"
#include "common/ComSmallDefs.h"

class ElemDDLUdrSurrogateLocation : public ElemDDLNode {
 public:
  // default constructor
  ElemDDLUdrSurrogateLocation(NAString &theLocation);

  // virtual destructor
  virtual ~ElemDDLUdrSurrogateLocation();

  // cast
  virtual ElemDDLUdrSurrogateLocation *castToElemDDLUdrSurrogateLocation(void);

  // accessor
  inline const NAString &getLocation(void) const { return locationName_; }

  //
  // methods for tracing
  //

  virtual const NAString displayLabel1() const;
  virtual NATraceList getDetailInfo() const;
  virtual const NAString getText() const;

 private:
  NAString &locationName_;

};  // class ElemDDLUdrSurrogateLocation

#endif /* ELEMDDLUDRSURROGATELOCATION_H */
