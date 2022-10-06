//******************************************************************************

//******************************************************************************
#ifndef ELEMDDLLIBPATHNAME_H
#define ELEMDDLLIBPATHNAME_H
/* -*-C++-*-
********************************************************************************
*
* File:         ElemDDLLibPathName.h
* Description:  class that contains the (parse node) elements in DDL statements
                for the path name of the deployed JAR file.
*
*
*
* Created:      11/12/2012
* Language:     C++
*
*
*
*
********************************************************************************
*/

#include "common/ComSmallDefs.h"
#include "ElemDDLNode.h"

class ElemDDLLibPathName : public ElemDDLNode {
 public:
  ElemDDLLibPathName(const NAString &theName);

  virtual ~ElemDDLLibPathName();

  virtual ElemDDLLibPathName *castToElemDDLLibPathName(void);

  inline const NAString &getPathName(void) const { return pathName_; }

  //
  // methods for tracing
  //

  virtual const NAString displayLabel1() const;
  NAString getSyntax() const;
  virtual const NAString getText() const;

 private:
  const NAString &pathName_;

};  // class ElemDDLLibPathName

#endif /* ELEMDDLLIBPATHNAME_H */
