// **********************************************************************

// **********************************************************************
#ifndef COM_PROBE_CACHE_H
#define COM_PROBE_CACHE_H

#include "comexe/ComTdb.h"

//
// Task Definition Block
//
class ComTdbProbeCache : public ComTdb {
  friend class ExProbeCacheTcb;
  friend class ExProbeCachePrivateState;

 protected:
  ComTdbPtr tdbChild_;         // 00-07
  ExExprPtr hashProbeExpr_;    // 08-15
  ExExprPtr encodeProbeExpr_;  // 16-23
  ExExprPtr moveInnerExpr_;    // 24-31
  ExExprPtr selectPred_;       // 32-39

  // length of the input probe
  UInt32 probeLen_;  // 40-43

  // number entries in the cache
  UInt32 cacheSize_;  // 44-47

  // length of the output row
  UInt32 recLen_;  // 48-51

  // index into atp of output row tupp
  UInt16 tuppIndex_;  // 52-53

  // workAtp indexes
  UInt16 hashValIdx_;           // 54-55
  UInt16 encodedProbeDataIdx_;  // 56-57
  UInt16 innerRowDataIdx_;      // 58-59

  UInt16 probeCacheFlags_;  // 60-61

  char fillersComTdbProbeCache1_[2];  // 62-63

  // size of "pool_" buffer, expressed
  // as # recs.
  UInt32 numInnerTuples_;  // 64-67

  char fillersComTdbProbeCache2_[12];  // 68-79

 public:
  // Constructor
  ComTdbProbeCache();  // dummy constructor. Used by 'unpack' routines.

  ComTdbProbeCache(ex_expr *hash_probe_expr, ex_expr *encode_probe_expr, ex_expr *move_inner_expr, ex_expr *select_pred,
                   int probe_len, int inner_rec_len, int cache_size, const unsigned short tupp_index,
                   const unsigned short hashValIdx, const unsigned short encodedProbeDataIdx,
                   const unsigned short innerRowDataIdx, ComTdb *child_tdb, ex_cri_desc *given_cri_desc,
                   ex_cri_desc *returned_cri_desc, queue_index down, queue_index up, Cardinality estimatedRowCount,
                   int numInnerTuples);

  ~ComTdbProbeCache();

  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(1, getClassVersionID());
    ComTdb::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(ComTdbProbeCache); }
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

  virtual const char *getNodeName() const { return "EX_PROBE_CACHE"; };

  virtual int numExpressions() const { return 4; }

  virtual ex_expr *getExpressionNode(int pos) {
    if (pos == 0)
      return hashProbeExpr_;
    else if (pos == 1)
      return encodeProbeExpr_;
    else if (pos == 2)
      return moveInnerExpr_;
    else if (pos == 3)
      return selectPred_;
    else
      return NULL;
  }

  virtual const char *getExpressionName(int pos) const {
    if (pos == 0)
      return "hashProbeExpr_";
    else if (pos == 1)
      return "encodeProbeExpr_";
    else if (pos == 2)
      return "moveInnerExpr_";
    else if (pos == 3)
      return "selectPred_";
    else
      return NULL;
  }
};

inline ComTdb *ComTdbProbeCache::getChildTdb() { return tdbChild_; };

/*****************************************************************************
  Description : Return ComTdb* depending on the position argument.
                  Position 0 means the left most child.
  Comments    :
  History     : Yeogirl Yun                                      8/22/95
                 Initial Revision.
*****************************************************************************/
inline const ComTdb *ComTdbProbeCache::getChild(int pos) const {
  if (pos == 0)
    return tdbChild_;
  else
    return NULL;
}

#endif
