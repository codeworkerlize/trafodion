
#ifndef STMTDDLDROPTRIGGER_H
#define STMTDDLDROPTRIGGER_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         StmtDDLDropTrigger.h
 * Description:  class for parse node representing Drop Trigger statements
 *
 *
 * Created:      2/2/98
 * Language:     C++
 * Status:       $State: Exp $
 *
 *****************************************************************************
 */

// -----------------------------------------------------------------------
// Change history:
//
// Revision 1.0  1998/02/02 03:21:44
// Initial revision
//
//
//
// -----------------------------------------------------------------------

#include "common/ComSmallDefs.h"
#include "parser/StmtDDLNode.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class StmtDDLDropTrigger;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None

// -----------------------------------------------------------------------
// Drop Trigger statement
// -----------------------------------------------------------------------
class StmtDDLDropTrigger : public StmtDDLNode {
 public:
  // constructor
  StmtDDLDropTrigger(const QualifiedName &tableQualName, NABoolean cleanupSpec, NABoolean validateSpec,
                     NAString *pLogFile);

  // virtual destructor
  virtual ~StmtDDLDropTrigger();

  // cast
  virtual StmtDDLDropTrigger *castToStmtDDLDropTrigger();

  // accessor
  inline const NAString getTriggerName() const;
  inline const QualifiedName &getTriggerNameAsQualifiedName() const;
  inline QualifiedName &getTriggerNameAsQualifiedName();
  inline const NABoolean isCleanupSpecified() const;
  inline const NABoolean isValidateSpecified() const;
  inline const NABoolean isLogFileSpecified() const;
  inline const NAString &getLogFile() const;

  // for binding
  ExprNode *bindNode(BindWA *bindWAPtr);

  // for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString getText() const;

 private:
  QualifiedName triggerQualName_;
  NABoolean isCleanupSpec_;
  NABoolean isValidateSpec_;
  NAString *pLogFile_;

};  // class StmtDDLDropTrigger

// -----------------------------------------------------------------------
// definitions of inline methods for class StmtDDLDropTrigger
// -----------------------------------------------------------------------

//
// accessor
//

inline QualifiedName &StmtDDLDropTrigger::getTriggerNameAsQualifiedName() { return triggerQualName_; }

inline const QualifiedName &StmtDDLDropTrigger::getTriggerNameAsQualifiedName() const { return triggerQualName_; }

inline const NAString StmtDDLDropTrigger::getTriggerName() const {
  return triggerQualName_.getQualifiedNameAsAnsiString();
}

inline const NABoolean StmtDDLDropTrigger::isCleanupSpecified() const { return isCleanupSpec_; }

inline const NABoolean StmtDDLDropTrigger::isValidateSpecified() const { return isValidateSpec_; }

inline const NABoolean StmtDDLDropTrigger::isLogFileSpecified() const {
  if (pLogFile_) return TRUE;
  return FALSE;
}

inline const NAString &StmtDDLDropTrigger::getLogFile() const {
  ComASSERT(pLogFile_ NEQ NULL);
  return *pLogFile_;
}

#endif  // STMTDDLDROPTRIGGER_H
