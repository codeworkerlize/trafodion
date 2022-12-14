
/* -*-C++-*-
****************************************************************************
*
* File:         ComTdbControl.h
* Description:
*
* Created:      5/6/98
* Language:     C++
*
*
*
*
****************************************************************************
*/

#ifndef COMTDBCONTROL_H
#define COMTDBCONTROL_H

#include "comexe/ComTdb.h"

class CliGlobals;
class Queue;

enum ControlQueryType {
  SHAPE_ = 0,
  DEFAULT_ = 1,
  TABLE_ = 2,
  CONTROL_SESSION_ = 3,
  SESSION_DEFAULT_ = 4,
  DEFAULTS_ = 5
};

///////////////////////////////////////////////////////
// class ExControlTdb
///////////////////////////////////////////////////////
class ComTdbControl : public ComTdb {
  friend class ExControlTcb;
  friend class ExControlPrivateState;

 public:
  enum ControlActionType { NONE_, HOLD_, RESTORE_ };

  ComTdbControl() : ComTdb(ComTdb::ex_CONTROL_QUERY, eye_CONTROL_QUERY), reset_(0), actionType_(NONE_) {}

  ComTdbControl(ControlQueryType cqt, int reset, char *sqlText, Int16 sqlTextCharSet, char *value1, char *value2,
                char *value3, ex_cri_desc *given_cri_desc, ex_cri_desc *returned_cri_desc, queue_index down,
                queue_index up, int num_buffers, int buffer_size);

  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(1, getClassVersionID());
    ComTdb::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(ComTdbControl); }

  Long pack(void *);
  int unpack(void *, void *reallocator);

  ControlQueryType getType() const { return (ControlQueryType)cqt_; }
  char *getSqlText() { return sqlText_; }
  Int16 getSqlTextCharSet() const { return sqlText_ ? sqlTextCharSet_ : (Int16)SQLCHARSETCODE_UNKNOWN; }
  char *getValue(int i) { return i == 1 ? value1_ : i == 2 ? value2_ : i == 3 ? value3_ : (NABasicPtr)NULL; }

  virtual int numChildren() const { return 0; }
  virtual const char *getNodeName() const { return "EX_CONTROL"; }
  virtual int numExpressions() const { return 0; }
  Int16 getControlActionType() { return actionType_; };
  void setControlActionType(Int16 actionType) { actionType_ = actionType; }
  void setNonResettable(NABoolean nonResettable) { nonResettable_ = nonResettable; }
  NABoolean isNonResettable() { return (nonResettable_ != FALSE); }

  void setIsSetStmt(short v) { (v ? flags_ |= IS_SET_STMT : flags_ &= ~IS_SET_STMT); };
  NABoolean isSetStmt() { return (flags_ & IS_SET_STMT) != 0; };

  void setIsHiveSetSchema(short v) { (v ? flags_ |= IS_HIVE_SET_SCHEMA : flags_ &= ~IS_HIVE_SET_SCHEMA); };
  NABoolean isHiveSetSchema() { return (flags_ & IS_HIVE_SET_SCHEMA) != 0; };

 private:
  enum { IS_SET_STMT = 0x0001, IS_HIVE_SET_SCHEMA = 0x0002 };

  // Remember, when putting tablename into one of these 3 char*'s,
  // to save its ANSI name; e.g.
  //	strcpy(xxx_, getTableName().getExposedNameAsAnsiString());

  // ## also need to add  charset  (enum? char*?) here -- i just did on 9/11/20101 - Thank you for the notes :-)
  // ## for SET CAT/SCH -- see ex_control.cpp ...

  NABasicPtr sqlText_;  // 00-07
  NABasicPtr value1_;   // 08-15
  NABasicPtr value2_;   // 16-23
  NABasicPtr value3_;   // 24-31

  Int16 cqt_;                      // 32-33
  Int16 reset_;                    // 34-35
  Int16 sqlTextCharSet_;           // 36-37
  Int16 actionType_;               // 38-39
  Int16 nonResettable_;            // 40-41
  UInt16 flags_;                   // 42-43
  char fillersComTdbControl_[28];  // 44-71
};
#endif
