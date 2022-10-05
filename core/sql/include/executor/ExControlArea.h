
#ifndef EXCONTROLAREA_H
#define EXCONTROLAREA_H
/* -*-C++-*-
****************************************************************************
*
* File:         ExControlArea.h
* Description:  The executor maintains a table of the dynamic CONTROL
*               statements that were issued by the current context.
*               This is done for two reasons: if arkcmp dies, we need
*               to send the new arkcmp all of the control statements,
*               and if we need to recompile a dynamic statement then
*               we need to find all the applicable dynamic CONTROL
*               statements (NOTE: we may not yet do the latter).
*
* Created:      5/6/98
* Language:     C++
*
****************************************************************************
*/

#include "comexe/ComTdbControl.h"

// -----------------------------------------------------------------------
// Contents of this file
// -----------------------------------------------------------------------

class ExControlEntry;
class ExControlArea;

// -----------------------------------------------------------------------
// Forward references
// -----------------------------------------------------------------------
class CliGlobals;
class Queue;

// -----------------------------------------------------------------------
// An entry in the Control Area (represents one CONTROL statement)
// -----------------------------------------------------------------------
class ExControlEntry : public NABasicObject {
 public:
  enum ResendType { UPON_ALL /*default */, UPON_CMP_CRASH, UPON_CTX_SWITCH };

 public:
  ExControlEntry(CollHeap *heap, ControlQueryType cqt, int reset = 0, char *sqlText = NULL, int lenX = 0,
                 Int16 sqlTextCharSet = (Int16)0 /*SQLCHARSETCODE_UNKNOWN*/, char *value1 = NULL, int len1 = 0,
                 char *value2 = NULL, int len2 = 0, char *value3 = NULL, int len3 = 0,
                 Int16 actionType = ComTdbControl::NONE_, ResendType resendType = ExControlEntry::UPON_ALL,
                 NABoolean isNonResettable = FALSE);

  ~ExControlEntry();

  ControlQueryType type() const { return cqt_; }
  int getNumValues() const { return numValues_; }
  int getReset() const { return reset_; }
  void setReset(int r) { reset_ = r; }
  char *getSqlText() { return sqlText_; }
  int getSqlTextLen() { return lenX_; }
  Int16 getSqlTextCharSet() { return sqlTextCharSet_; }
  char *getValue(int i);
  int getLen(int i);
  int match(ControlQueryType cqt, const char *value1, const char *value2, int reset = 0);

  ResendType getResendType();
  Int16 getActionType() { return actionType_; }
  NABoolean isNonResettable() { return nonResettable_; }

 private:
  ResendType resendType_;
  CollHeap *heap_;
  ControlQueryType cqt_;
  int reset_;

  int numValues_;

  char *sqlText_;
  Int16 sqlTextCharSet_;
  Int16 actionType_;
  char *value1_;
  char *value2_;
  char *value3_;
  int lenX_;
  int len1_;
  int len2_;
  int len3_;
  NABoolean nonResettable_;
};

// -----------------------------------------------------------------------
// The area (list) of CONTROL statements issued so far
// -----------------------------------------------------------------------
class ExControlArea : public NABasicObject {
 public:
  ExControlArea(ContextCli *context, CollHeap *heap);

  ~ExControlArea();

  void addControl(ControlQueryType type, int reset = 0, const char *sqlText = NULL, int lenX = 0,
                  const char *value1 = NULL, int len1 = 0, const char *value2 = NULL, int len2 = 0,
                  const char *value3 = NULL, int len3 = 0, Int16 actionType = ComTdbControl::NONE_,
                  ExControlEntry::ResendType resendType = ExControlEntry::UPON_ALL, NABoolean isNonResettable = FALSE);
  Queue *getControlList() { return controlList_; }

  static const char *getText(ControlQueryType cqt);

 private:
  ContextCli *context_;
  CollHeap *heap_;
  Queue *controlList_;
  void *resetAllQueueEntry_;
  void *sysDefResetQueueEntry_;
};

#endif /* EXCONTROLAREA_H */
