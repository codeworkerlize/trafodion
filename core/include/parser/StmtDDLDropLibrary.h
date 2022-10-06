#ifndef STMTDDLDROPLIBRARY_H
#define STMTDDLDROPLIBRARY_H
//******************************************************************************

//******************************************************************************
/* -*-C++-*-
********************************************************************************
*
* File:         StmtDDLDropLibrary.h
* Description:  class for parse node representing Drop Library statements
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
#include "parser/StmtDDLNode.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLDropLibrary;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None

// -----------------------------------------------------------------------
// Drop Library statement
// -----------------------------------------------------------------------
class StmtDDLDropLibrary : public StmtDDLNode {
 public:
  StmtDDLDropLibrary();
  StmtDDLDropLibrary(const QualifiedName &libraryName, ComDropBehavior dropBehavior);

  virtual ~StmtDDLDropLibrary();

  virtual StmtDDLDropLibrary *castToStmtDDLDropLibrary();

  inline const NAString getLibraryName() const;
  inline ComDropBehavior getDropBehavior() const;

  inline const QualifiedName &getLibraryNameAsQualifiedName() const;
  inline QualifiedName &getLibraryNameAsQualifiedName();

  // for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString getText() const;

  // for binding
  ExprNode *bindNode(BindWA *bindWAPtr);

 private:
  QualifiedName libraryName_;
  ComDropBehavior dropBehavior_;

};  // class StmtDDLDropLibrary

// -----------------------------------------------------------------------
// definitions of inline methods for class StmtDDLDropLibrary
// -----------------------------------------------------------------------

//
// accessors
//

inline ComDropBehavior StmtDDLDropLibrary::getDropBehavior() const { return dropBehavior_; }

inline const NAString StmtDDLDropLibrary::getLibraryName() const {
  NAString libraryName = libraryName_.getQualifiedNameAsAnsiString();

  return libraryName;
}

inline QualifiedName &StmtDDLDropLibrary::getLibraryNameAsQualifiedName() { return libraryName_; }

inline const QualifiedName &StmtDDLDropLibrary::getLibraryNameAsQualifiedName() const { return libraryName_; }

#endif  // STMTDDLDROPLIBRARY_H
