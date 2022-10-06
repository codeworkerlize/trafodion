
#ifndef COM_CONNECT_BY_TEMP_TABLE
#define COM_CONNECT_BY_TEMP_TABLE

#include "comexe/ComTdb.h"

class ComTdbConnectByTempTable : public ComTdb {
  friend class ExConnectByTempTableTcb;

 public:
  ComTdbConnectByTempTable();

  ComTdbConnectByTempTable(ComTdb *child_tdb, ex_expr *hash_probe_expr, ex_expr *encode_probe_expr,
                           ex_expr *move_inner_expr, int probe_len, int inner_rec_len, int cache_size,
                           const unsigned short tupp_index, const unsigned short hashValIdx,
                           const unsigned short encodedProbeDataIdx, const unsigned short innerRowDataIdx,
                           ex_cri_desc *workCriDesc, ex_cri_desc *givenCriDesc, ex_cri_desc *returnedCriDesc,
                           queue_index down, queue_index up, int numBuffers, int bufferSize,
                           ex_expr *encodeInputHostVarExpr, ex_expr *hvExprInput, UInt16 hashInputValIdx,
                           UInt16 encodeInputProbeDataIdx, ex_expr *scanExpr);

  ~ComTdbConnectByTempTable();

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

  virtual short getClassSize() { return (short)sizeof(ComTdbConnectByTempTable); }

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
  virtual const char *getNodeName() const { return "EX_CONNECT_BY_TEMP_TABLE"; };
  virtual int numExpressions() const { return 6; };
  virtual ex_expr *getExpressionNode(int pos) {
    switch (pos) {
      case 0:
        return hashProbeExpr_;
      case 1:
        return encodeProbeExpr_;
      case 2:
        return moveInnerExpr_;
      case 3:
        return hashInputExpr_;
      case 4:
        return encodeInputExpr_;
      case 5:
        return scanExpr_;
    }
    return NULL;
  };
  virtual const char *getExpressionName(int pos) const {
    switch (pos) {
      case 0:
        return "hashProbeExpr_";
      case 1:
        return "encodeProbeExpr_";
      case 2:
        return "moveInnerExpr_";
      case 3:
        return "hashInputExpr_";
      case 4:
        return "encodeInputExpr_";
      case 5:
        return "scanExpr_";
    }
    return NULL;
  };

  // ---------------------------------------------------------------------
  // Used by the internal SHOWPLAN command to get attributes of a TDB.
  // ---------------------------------------------------------------------
  virtual void displayContents(Space *space, int flag);

 protected:
  ComTdbPtr tdbChild_;  // 08-15
  ExExprPtr hashProbeExpr_;
  ExExprPtr encodeProbeExpr_;
  ExExprPtr moveInnerExpr_;
  ExExprPtr hashInputExpr_;
  ExExprPtr encodeInputExpr_;
  ExCriDescPtr workCriDesc_;  // 24-31
  UInt32 probeLen_;
  UInt32 cacheSize_;
  UInt32 recLen_;
  UInt16 tuppIndex_;
  UInt16 hashValIdx_;
  UInt16 encodedProbeDataIdx_;
  UInt16 innerRowDataIdx_;
  UInt16 hashInputValIdx_;
  UInt16 encodeInputProbeDataIdx_;
  char filler0ComTdbConnectByTempTable_[32];  // 32-63  unused
  ExExprPtr scanExpr_;
};

#endif
