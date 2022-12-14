
#ifndef _CLI_MSG_OBJ_H_
#define _CLI_MSG_OBJ_H_

// forwarded class declarations
class ComplexObject;

class CtrlStmtComplexObject : public ComplexObject {
 public:
  CtrlStmtComplexObject(NAMemory *heap, char *sqlText, Int16 sqlTextCharSet, CtrlStmtComplexObject *ctrlObj);
  CtrlStmtComplexObject(char *sqlText, Int16 sqlTextCharSet, CtrlStmtComplexObject *ctrlObj);
  CtrlStmtComplexObject(NAMemory *heap);
  CtrlStmtComplexObject();
  virtual ~CtrlStmtComplexObject();
  virtual void freeSubObjects();
  virtual void sharedOperationSequence(MessageOperator *msgOp, InputContainer *input, OutputContainer *output);
  char *getSqlText();
  Int16 getSqlTextCharSet();
  CtrlStmtComplexObject *getCtrlStmt();
  void dump();

 private:
  char *sqlText_;
  Int16 sqlTextCharSet_;
  CtrlStmtComplexObject *ctrlObj_;
};

class TransAttrComplexObject : public ComplexObject {
 public:
  TransAttrComplexObject(NAMemory *heap, TransMode::AccessMode mode, TransMode::IsolationLevel isoLv, int diagSize,
                         TransMode::RollbackMode rollbackMode, int autoabortInterval);
  TransAttrComplexObject(TransMode::AccessMode mode, TransMode::IsolationLevel isoLv, int diagSize,
                         TransMode::RollbackMode rollbackMode, int autoabortInterval);
  TransAttrComplexObject(NAMemory *heap);
  TransAttrComplexObject();
  virtual ~TransAttrComplexObject();
  virtual void freeSubObjects();
  virtual void sharedOperationSequence(MessageOperator *msgOp, InputContainer *input, OutputContainer *output);
  TransMode::AccessMode getAccessMode();
  TransMode::IsolationLevel getIsolationLevel();
  int getDiagSize();
  TransMode::RollbackMode getRollbackMode();
  int getAutoabortInterval();
  void setAccessMode(TransMode::AccessMode mode);
  void setRollbackMode(TransMode::RollbackMode rollbackMode);
  void setIsolationLevel(TransMode::IsolationLevel isoLv);
  void setDiagSize(int diagSize);
  void setAutoabortInterval(int autoabortInterval);
  void dump();

 private:
  TransMode::AccessMode mode_;
  TransMode::IsolationLevel isoLv_;
  int diagSize_;
  TransMode::RollbackMode rollbackMode_;
  int autoabortInterval_;
};

// Stack allocated factory object.
// Should used singleton pattern. However, NSK security does not allow globals.
class CliComplexObjectFactory : public ComplexObjectFactory {
 public:
  virtual ComplexObject *manufacture(NAMemory *heap, ComplexObjectType objType);
};
#endif
