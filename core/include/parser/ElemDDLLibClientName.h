//******************************************************************************

//******************************************************************************
#ifndef ELEMDDLLIBCLIENTNAME_H
#define ELEMDDLLIBCLIENTNAME_H
/* -*-C++-*-
********************************************************************************
*
* File:         ElemDDLLibClientName.h
* Description:  class that contains the (parse node) elements in DDL statements
                for name of client that deployed a library
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

#include "parser/ElemDDLNode.h"
#include "common/ComSmallDefs.h"

class ElemDDLLibClientName : public ElemDDLNode {
 public:
  ElemDDLLibClientName(const NAString &theName);

  virtual ~ElemDDLLibClientName();

  virtual ElemDDLLibClientName *castToElemDDLLibClientName(void);

  inline const NAString &getClientName(void) const { return clientName_; }

  //
  // methods for tracing
  //

  virtual const NAString displayLabel1() const;
  NAString getSyntax() const;
  virtual const NAString getText() const;

 private:
  const NAString &clientName_;

};  // class ElemDDLLibClientName

#endif /* ELEMDDLLIBCLIENTNAME_H */
