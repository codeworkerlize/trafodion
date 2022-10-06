
/* -*-C++-*-
****************************************************************************
*
* File:         ComTdbConnectBy.h
* Description:
*
* Created:      8/2/2019
* Language:     C++
*
*
*
*
****************************************************************************
*/

#ifndef COM_CONNECTBY_H
#define COM_CONNECTBY_H

#include "comexe/ComQueue.h"
#include "comexe/ComTdb.h"

class ComTdbConnectBy : public ComTdb {
  friend class ExConnectByTcb;

 public:
  ComTdbConnectBy();

  ComTdbConnectBy(ex_cri_desc *workCriDesc, ex_cri_desc *givenCriDesc, ex_cri_desc *returnedCriDesc, queue_index down,
                  queue_index up, int numBuffers, int bufferSize, ComTdb *s_child_tdb, ComTdb *c_child_tdb,
                  UInt32 outputRowLen, UInt32 pseudoOutputRowLen, ex_expr *leftMoveExpr, ex_expr *rightMoveExpr,
                  short returnRowAtpIndex, short fixedPseudoColRowAtpIndex, ex_expr *priorPredExpr,
                  short priorPredAtpIndex, UInt32 priorPredHostVarLen, ex_expr *priorValMoveExpr1,
                  ex_expr *priorValMoveExpr2, short priorValsValsDownAtpIndex, ex_cri_desc *rightDownCriDesc,
                  ex_expr *condExpr, short pathExprAtpIndex, ex_expr *leftPathExpr, ex_expr *rightPathExpr,
                  short pathPseudoColRowAtpIndex, UInt32 pathOutputRowLen, UInt32 pathItemRowLen,
                  ex_expr *priorCondExpr);

  ~ComTdbConnectBy();

  int orderedQueueProtocol() const { return -1; };

  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(1, getClassVersionID());
    ComTdb::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(ComTdbConnectBy); }

  virtual Long pack(void *);
  virtual int unpack(void *, void *reallocator);

  void display() const;

  virtual int numChildren() const { return 2; }
  virtual const char *getNodeName() const { return "EX_CONNECTBY"; };

  virtual const ComTdb *getChild(int pos) const {
    if (pos == 0)
      return tdbSChild_.getPointer();
    else
      return tdbCChild_.getPointer();
  }
  virtual int numExpressions() const { return 8; };
  virtual ex_expr *getExpressionNode(int pos);
  virtual const char *getExpressionName(int pos) const;  // { return "firstNRowsExpr"; };

  void setDel(NAString d) { del_ = d; }
  void setNoCycle(NABoolean b) { nocycle_ = b; }
  NABoolean noCycle() { return nocycle_; }
  void setNoPrior(NABoolean b) { noPrior_ = b; }
  void setUseCache(NABoolean b) { useCache_ = b; }
  NABoolean useCache() { return useCache_; }
  NABoolean noPrior() { return noPrior_; }

  // ---------------------------------------------------------------------
  // Used by the internal SHOWPLAN command to get attributes of a TDB.
  // ---------------------------------------------------------------------
  virtual void displayContents(Space *space, int flag);

  ComTdb *getChild(int i) {
    if (i == 0)
      return tdbSChild_;
    else if (i == 1)
      return tdbCChild_;
    else
      return NULL;
  }

 protected:
  ComTdbPtr tdbSChild_;       // 0-7
  ComTdbPtr tdbCChild_;       // 8-15
  ExCriDescPtr workCriDesc_;  // 16-23
  UInt32 outputRowLen_;
  UInt32 pseudoOutputRowLen_;
  UInt32 priorPredHostVarLen_;
  ExExprPtr leftMoveExpr_;
  ExExprPtr rightMoveExpr_;
  ExExprPtr priorPredExpr_;
  ExExprPtr priorValMoveExpr1_;
  ExExprPtr priorValMoveExpr2_;
  short returnRowAtpIndex_;
  short fixedPseudoColRowAtpIndex_;
  short priorPredAtpIndex_;
  short priorValsValsDownAtpIndex_;
  ExCriDescPtr rightDownCriDesc_;
  ExExprPtr condExpr_;
  ExExprPtr priorCondExpr_;
  ExExprPtr leftPathExpr_;
  ExExprPtr rightPathExpr_;
  short pathExprAtpIndex_;
  short pathPseudoColRowAtpIndex_;
  UInt32 pathOutputRowLen_;
  UInt32 pathItemLength_;
  NAString del_;
  NABoolean nocycle_;
  NABoolean noPrior_;
  NABoolean useCache_;
};

#endif
