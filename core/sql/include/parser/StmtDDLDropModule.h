#ifndef STMTDDLDROPMODULE_H
#define STMTDDLDROPMODULE_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLDropModule.h
 * Description:  class for parse node representing Drop Module statements
 *
 *
 * Created:      03/28/96
 * Language:     C++
 *
 *

 *
 *
 *****************************************************************************
 */

#include "common/ComSmallDefs.h"
#include "parser/StmtDDLNode.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLDropModule;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None

// -----------------------------------------------------------------------
// Drop Alias statement
// -----------------------------------------------------------------------
class StmtDDLDropModule : public StmtDDLNode {
 public:
  // constructor
  StmtDDLDropModule(const QualifiedName &modulename);

  // virtual destructor
  virtual ~StmtDDLDropModule();

  // cast
  virtual StmtDDLDropModule *castToStmtDDLDropModule();

  // accessors

  inline const NAString &getModuleName() const;
  inline const QualifiedName &getModuleNameAsQualifiedName() const;
  inline QualifiedName getModuleNameAsQualifiedName();

  // for binding
  ExprNode *bindNode(BindWA *bindWAPtr);

  // for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString getText() const;

 private:
  NAString moduleName_;
  QualifiedName moduleQualName_;
};  // class StmtDDLDropModule

// -----------------------------------------------------------------------
// definitions of inline methods for class StmtDDLDropModule
// -----------------------------------------------------------------------

//
// accessors
//

inline QualifiedName StmtDDLDropModule::getModuleNameAsQualifiedName() { return moduleQualName_; }

inline const QualifiedName &StmtDDLDropModule::getModuleNameAsQualifiedName() const { return moduleQualName_; }

inline const NAString &StmtDDLDropModule::getModuleName() const { return moduleName_; }
#endif  // STMTDDLDROPMODULE_H
