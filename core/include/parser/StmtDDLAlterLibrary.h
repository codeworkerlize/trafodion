#ifndef STMTDDLALTERLIBRARY_H
#define STMTDDLALTERLIBRARY_H
//******************************************************************************

//******************************************************************************
/*
********************************************************************************
*
* File:         StmtDDLAlterLibrary.h
* Description:  class for parse node representing alter Library statements
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

class StmtDDLAlterLibrary : public StmtDDLNode

{
 public:
  StmtDDLAlterLibrary();
  StmtDDLAlterLibrary(const QualifiedName &libraryName, const NAString &libraryFilename, ElemDDLNode *clientName,
                      ElemDDLNode *clientFilename, CollHeap *heap);

  virtual ~StmtDDLAlterLibrary();

  virtual StmtDDLAlterLibrary *castToStmtDDLAlterLibrary();

  //
  // method for binding
  //

  ExprNode *bindNode(BindWA *bindWAPtr);

  // accessors

  inline const NAString getLibraryName() const;
  inline const NAString getFilename() const;
  inline const NAString &getClientName() const;
  inline const NAString &getClientFilename() const;
  inline const QualifiedName &getLibraryNameAsQualifiedName() const;
  inline QualifiedName &getLibraryNameAsQualifiedName();

  // for tracing

  virtual const NAString displayLabel1() const;
  virtual const NAString displayLabel2() const;
  virtual const NAString getText() const;

 private:
  QualifiedName libraryName_;
  const NAString &fileName_;
  NAString clientName_;
  NAString clientFilename_;
};

//----------------------------------------------------------------------------
// definitions of inline methods for class StmtDDLAlterLibrary
//----------------------------------------------------------------------------

//
// accessors
//

inline const NAString StmtDDLAlterLibrary::getLibraryName() const {
  return libraryName_.getQualifiedNameAsAnsiString();
}

inline const NAString StmtDDLAlterLibrary::getFilename() const { return fileName_; }

inline const NAString &StmtDDLAlterLibrary::getClientName() const { return clientName_; }

inline const NAString &StmtDDLAlterLibrary::getClientFilename() const { return clientFilename_; }

inline QualifiedName &StmtDDLAlterLibrary::getLibraryNameAsQualifiedName() { return libraryName_; }

inline const QualifiedName &StmtDDLAlterLibrary::getLibraryNameAsQualifiedName() const { return libraryName_; }
#endif  // STMTDDLALTERLIBRARY
