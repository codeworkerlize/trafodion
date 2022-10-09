//******************************************************************************

//******************************************************************************
#ifndef ELEMDDLUDRLIBRARY_H
#define ELEMDDLUDRLIBRARY_H
/* -*-C++-*-
******************************************************************************
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
******************************************************************************
*/

#include "parser/ElemDDLNode.h"
#include "common/ComSmallDefs.h"

class ElemDDLUdrLibrary : public ElemDDLNode {
 public:
  // default constructor
  ElemDDLUdrLibrary(QualifiedName &theLibrary);

  // virtual destructor
  virtual ~ElemDDLUdrLibrary();

  // cast
  virtual ElemDDLUdrLibrary *castToElemDDLUdrLibrary(void);

  // accessor
  inline const QualifiedName &getLibraryName(void) const { return libraryName_; }

  // methods for binding.
  virtual ExprNode *bindNode(BindWA *pBindWA);

  //
  // methods for tracing
  //

  virtual const NAString displayLabel1() const;
  virtual NATraceList getDetailInfo() const;
  virtual const NAString getText() const;

 private:
  QualifiedName &libraryName_;

};  // class ElemDDLUdrLibrary

#endif /* ELEMDDLUDRLIBRARY_H */
