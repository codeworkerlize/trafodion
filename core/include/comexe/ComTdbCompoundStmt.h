
#ifndef ComCompoundStmt_h
#define ComCompoundStmt_h

/* -*-C++-*-
******************************************************************************
*
* File: 	ComTdbCompoundStmt.h
* Description:  3GL compound statement (CS) operator.
*
* Created:      4/1/98
* Language:     C++
*
*
*
******************************************************************************
*/

#include "comexe/ComTdb.h"

//////////////////////////////////////////////////////////////////////////////
//
// CompoundStmt TDB class.
//
//////////////////////////////////////////////////////////////////////////////
class ComTdbCompoundStmt : public ComTdb {
  friend class ExCatpoundStmtTcb;
  friend class ExCatpoundStmtPrivateState;

 public:
  //
  // Standard TDB methods.
  //
  ComTdbCompoundStmt();

  ComTdbCompoundStmt(ComTdb *left, ComTdb *right, ex_cri_desc *given, ex_cri_desc *returned, queue_index down,
                     queue_index up, int numBuffers, int bufferSize, NABoolean rowsFromLeft, NABoolean rowsFromRight,
                     NABoolean AfterUpdate);

  // exclude from code coverage analsysis since this method is not used
  int orderedQueueProtocol() const { return -1; }

  Long pack(void *);
  int unpack(void *, void *reallocator);

  void display() const {};

  virtual const ComTdb *getChild(int pos) const;

  virtual int numChildren() const { return 2; }
  virtual int numExpressions() const { return 0; }

  virtual const char *getNodeName() const { return "EX_COMPOUND_STMT"; };

  // exclude from code coverage analysys since there are no expressions for
  // this operator
  virtual ex_expr *getExpressionNode(int pos) { return NULL; }

  virtual const char *getExpressionName(int pos) const { return NULL; }

  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(1, getClassVersionID());
    ComTdb::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }

 protected:
  //
  // CS TDB specific attributes.
  //

  enum { ROWS_FROM_LEFT = 0x0001, ROWS_FROM_RIGHT = 0x0002, AFTER_UPDATE = 0x0004 };

  ComTdbPtr tdbLeft_;   // TDB for left child.  00-07
  ComTdbPtr tdbRight_;  // TDB for right child. 08-15

  UInt16 flags_;  // 16-17

  char fillersComTdbCompound_[30];  // 18-47

  inline NABoolean expectingLeftRows() const { return (flags_ & ROWS_FROM_LEFT) != 0; }

  inline NABoolean expectingRightRows() const { return (flags_ & ROWS_FROM_RIGHT) != 0; }

  inline NABoolean afterUpdate() const { return (flags_ & AFTER_UPDATE) != 0; }
};

#endif  // ComCompoundStmt_h
