//******************************************************************************

//******************************************************************************
#ifndef ELEMDDLLIBCLIENTFILENAME_H
#define ELEMDDLLIBCLIENTFILENAME_H
/* -*-C++-*-
********************************************************************************
*
* File:         ElemDDLLibClientFilename.h
* Description:  class that contains the (parse node) elements in DDL statements
                for the local filename of the deployed library
*
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

#include "common/ComSmallDefs.h"
#include "ElemDDLNode.h"

class ElemDDLLibClientFilename : public ElemDDLNode {
 public:
  ElemDDLLibClientFilename(const NAString &theFilename);

  virtual ~ElemDDLLibClientFilename();

  virtual ElemDDLLibClientFilename *castToElemDDLLibClientFilename(void);

  inline const NAString &getFilename(void) const { return theFilename_; }

  //
  // methods for tracing
  //

  virtual const NAString displayLabel1() const;
  NAString getSyntax() const;
  virtual const NAString getText() const;

 private:
  const NAString &theFilename_;

};  // class ElemDDLLibClientFilename

#endif /* ELEMDDLLIBCLIENTFILENAME_H */
