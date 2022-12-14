
/* -*-C++-*-
****************************************************************************
*
* File:         ComTdbFirstN.h
* Description:
*
* Created:      5/2/2003
* Language:     C++
*
*
*
*
****************************************************************************
*/

#ifndef COM_FIRSTN_H
#define COM_FIRSTN_H

#include "comexe/ComTdb.h"

class ComTdbFirstN : public ComTdb {
  friend class ExFirstNTcb;

 public:
  ComTdbFirstN();

  ComTdbFirstN(ComTdb *child_tdb, long firstNRows, ex_expr *firstNRowsExpr, ex_cri_desc *workCriDesc,
               ex_cri_desc *givenCriDesc, ex_cri_desc *returnedCriDesc, queue_index down, queue_index up,
               int numBuffers, int bufferSize);

  ~ComTdbFirstN();

  int orderedQueueProtocol() const { return -1; };

  ComTdb *getChildTdb() { return tdbChild_; };

  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(1, getClassVersionID());
    ComTdb::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(ComTdbFirstN); }

  virtual Long pack(void *);
  virtual int unpack(void *, void *reallocator);

  void display() const;

  virtual const ComTdb *getChild(int pos) const {
    if (pos == 0)
      return tdbChild_.getPointer();
    else
      return NULL;
  }

  virtual int numChildren() const { return 1; }
  virtual const char *getNodeName() const { return "EX_FIRSTN"; };
  virtual int numExpressions() const { return 1; };
  virtual ex_expr *getExpressionNode(int pos) { return firstNRowsExpr_; };
  virtual const char *getExpressionName(int pos) const { return "firstNRowsExpr"; };

  // ---------------------------------------------------------------------
  // Used by the internal SHOWPLAN command to get attributes of a TDB.
  // ---------------------------------------------------------------------
  virtual void displayContents(Space *space, int flag);

  long firstNRows() { return firstNRows_; }

 protected:
  long firstNRows_;               // 00-07
  ComTdbPtr tdbChild_;            // 08-15
  ExExprPtr firstNRowsExpr_;      // 16-23
  ExCriDescPtr workCriDesc_;      // 24-31
  char filler0ComTdbFirstN_[32];  // 32-63  unused
};

#endif
