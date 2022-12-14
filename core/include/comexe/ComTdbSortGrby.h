
/* -*-C++-*-
****************************************************************************
*
* File:         ComTdbSortGrby.h
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

#ifndef COM_SORT_GRBY_H
#define COM_SORT_GRBY_H

#include "comexe/ComTdb.h"

//
// Task Definition Block
//
class ComTdbSortGrby : public ComTdb {
  friend class ex_sort_grby_tcb;
  friend class ex_sort_grby_rollup_tcb;
  friend class ex_sort_grby_private_state;

 protected:
  enum { IS_ROLLUP = 0x0001 };

  ComTdbPtr tdbChild_;    // 00-07
  ExExprPtr aggrExpr_;    // 08-15
  ExExprPtr grbyExpr_;    // 16-23
  ExExprPtr moveExpr_;    // 24-31
  ExExprPtr havingExpr_;  // 32-39

  // length of the aggregated row
  int recLen_;  // 40-43

  // index into atp of new sort_grby tupp
  UInt16 tuppIndex_;  // 44-45

  UInt16 flags_;  // 46-47

  Int16 numRollupGroups_;  // 48-49

  char fillersComTdbSortGrby_[38];  // 50-87

 public:
  // Constructor
  ComTdbSortGrby();  // dummy constructor. Used by 'unpack' routines.

  ComTdbSortGrby(ex_expr *aggr_expr, ex_expr *grby_expr, ex_expr *move_expr, ex_expr *having_expr, int reclen,
                 const unsigned short tupp_index, ComTdb *child_tdb, ex_cri_desc *given_cri_desc,
                 ex_cri_desc *returned_cri_desc, queue_index down, queue_index up, Cardinality estimatedRowCount,
                 int num_buffers, int buffer_size, NABoolean tolerateNonFatalError);

  ~ComTdbSortGrby();

  AggrExpr *aggrExpr() { return (AggrExpr *)((ex_expr *)aggrExpr_); }

  void setNumRollupGroups(Int16 v) { numRollupGroups_ = v; }
  Int16 numRollupGroups() { return numRollupGroups_; }

  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(1, getClassVersionID());
    ComTdb::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(ComTdbSortGrby); }
  Long pack(void *);
  int unpack(void *, void *reallocator);

  void display() const;

  inline ComTdb *getChildTdb();

  int orderedQueueProtocol() const;

  // ---------------------------------------------------------------------
  // Used by the internal SHOWPLAN command to get attributes of a TDB.
  // ---------------------------------------------------------------------
  virtual void displayContents(Space *space, int flag);

  virtual const ComTdb *getChild(int pos) const;
  virtual int numChildren() const { return 1; }
  virtual const char *getNodeName() const { return "EX_SORT_GRBY"; };
  virtual int numExpressions() const { return 4; }
  virtual ex_expr *getExpressionNode(int pos) {
    if (pos == 0)
      return aggrExpr_;
    else if (pos == 1)
      return grbyExpr_;
    else if (pos == 2)
      return moveExpr_;
    else if (pos == 3)
      return havingExpr_;
    else
      return NULL;
  }
  virtual const char *getExpressionName(int pos) const {
    if (pos == 0)
      return "aggrExpr_";
    else if (pos == 1)
      return "grbyExpr_";
    else if (pos == 2)
      return "moveExpr_";
    else if (pos == 3)
      return "havingExpr_";
    else
      return NULL;
  }

  NABoolean isRollup() { return ((flags_ & IS_ROLLUP) != 0); };
  void setIsRollup(NABoolean v) { (v ? flags_ |= IS_ROLLUP : flags_ &= ~IS_ROLLUP); }
};

inline ComTdb *ComTdbSortGrby::getChildTdb() { return tdbChild_; };

/*****************************************************************************
  Description : Return ComTdb* depending on the position argument.
                  Position 0 means the left most child.
  Comments    :
  History     :
                 Initial Revision.
*****************************************************************************/
inline const ComTdb *ComTdbSortGrby::getChild(int pos) const {
  if (pos == 0)
    return tdbChild_;
  else
    return NULL;
}

#endif
