
#ifndef STMTDDLDROPROUTINE_H
#define STMTDDLDROPROUTINE_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLDropRoutine.h
 * Description:  class for parse node representing Drop Routine statements
 *
 *
 * Created:      9/30/1999
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "common/ComSmallDefs.h"
#include "parser/StmtDDLNode.h"
#define SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#include "parser/SqlParserGlobals.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLDropRoutine;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None

// -----------------------------------------------------------------------
// Drop Package statement
// -----------------------------------------------------------------------
class StmtDDLDropPackage : public StmtDDLNode {
 public:
  StmtDDLDropPackage(const QualifiedName &packageQualName, NABoolean cleanupSpec, CollHeap *heap)
      : StmtDDLNode(DDL_DROP_PACKAGE), isCleanupSpec_(cleanupSpec), packageQualName_(packageQualName, heap) {}

  virtual ~StmtDDLDropPackage() {}

  virtual StmtDDLDropPackage *castToStmtDDLDropPackage() { return this; }

  const QualifiedName &getPackageNameAsQualifiedName() { return packageQualName_; }

  const NAString getPackageName() const { return packageQualName_.getQualifiedNameAsAnsiString(); }

  const NABoolean isCleanupSpecified() const { return isCleanupSpec_; }
  const NABoolean dropIfExists() const { return dropIfExists_; }
  void setDropIfExists(NABoolean v) { dropIfExists_ = v; }

  ExprNode *bindNode(BindWA *pBindWA);

 private:
  QualifiedName packageQualName_;
  NABoolean isCleanupSpec_;
  NABoolean dropIfExists_;
};

// -----------------------------------------------------------------------
// Create Catalog statement
// -----------------------------------------------------------------------
class StmtDDLDropRoutine : public StmtDDLNode {
 public:
  // constructor
  StmtDDLDropRoutine(ComRoutineType routineType, const QualifiedName &routineQualName,
                     const QualifiedName &routineActionQualName, ComDropBehavior dropBehavior, NABoolean cleanupSpec,
                     NABoolean validateSpec, NAString *pLogFile, CollHeap *heap = PARSERHEAP());

  // virtual destructor
  virtual ~StmtDDLDropRoutine();

  // cast
  virtual StmtDDLDropRoutine *castToStmtDDLDropRoutine();

  // accessors
  inline ComRoutineType getRoutineType() const;
  inline ComDropBehavior getDropBehavior() const;
  inline const NAString getRoutineName() const;
  inline const QualifiedName &getRoutineNameAsQualifiedName() const;
  inline QualifiedName &getRoutineNameAsQualifiedName();
  inline const NAString getRoutineActionName() const;
  inline const QualifiedName &getRoutineActionNameAsQualifiedName() const;
  inline QualifiedName &getRoutineActionNameAsQualifiedName();
  inline const NABoolean isCleanupSpecified() const;
  inline const NABoolean isValidateSpecified() const;
  inline const NABoolean isLogFileSpecified() const;
  inline const NAString &getLogFile() const;

  const NABoolean dropIfExists() const { return dropIfExists_; }
  void setDropIfExists(NABoolean v) { dropIfExists_ = v; }

  // for binding
  ExprNode *bindNode(BindWA *bindWAPtr);

  // for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString displayLabel2() const;
  virtual const NAString getText() const;

 private:
  ComRoutineType routineType_;
  QualifiedName routineQualName_;
  QualifiedName routineActionQualName_;
  ComDropBehavior dropBehavior_;
  NABoolean isCleanupSpec_;
  NABoolean isValidateSpec_;
  NAString *pLogFile_;
  NABoolean dropIfExists_;

};  // class StmtDDLDropRoutine

// -----------------------------------------------------------------------
// definitions of inline methods for class StmtDDLDropRoutine
// -----------------------------------------------------------------------

//
// accessors
//

inline ComRoutineType StmtDDLDropRoutine::getRoutineType() const { return routineType_; }

inline QualifiedName &StmtDDLDropRoutine::getRoutineNameAsQualifiedName() { return routineQualName_; }

inline const QualifiedName &StmtDDLDropRoutine::getRoutineNameAsQualifiedName() const { return routineQualName_; }

inline QualifiedName &StmtDDLDropRoutine::getRoutineActionNameAsQualifiedName() { return routineActionQualName_; }

inline const QualifiedName &StmtDDLDropRoutine::getRoutineActionNameAsQualifiedName() const {
  return routineActionQualName_;
}

inline ComDropBehavior StmtDDLDropRoutine::getDropBehavior() const { return dropBehavior_; }

inline const NAString StmtDDLDropRoutine::getRoutineName() const {
  return routineQualName_.getQualifiedNameAsAnsiString();
}

inline const NAString StmtDDLDropRoutine::getRoutineActionName() const {
  return routineActionQualName_.getQualifiedNameAsAnsiString();
}

inline const NABoolean StmtDDLDropRoutine::isCleanupSpecified() const { return isCleanupSpec_; }

inline const NABoolean StmtDDLDropRoutine::isValidateSpecified() const { return isValidateSpec_; }

inline const NABoolean StmtDDLDropRoutine::isLogFileSpecified() const {
  if (pLogFile_) return TRUE;
  return FALSE;
}

inline const NAString &StmtDDLDropRoutine::getLogFile() const {
  ComASSERT(pLogFile_ NEQ NULL);
  return *pLogFile_;
}

#endif  // STMTDDLDROPROUTINE_H
