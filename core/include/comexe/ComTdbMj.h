// **********************************************************************

// -*-C++-*-
// ************************************************************************
// *
// * File:         ComTdbMj.h
// * Description:
// *
// * Created:      5/6/98
// * Language:     C++
// *
// *
// *
// *
// *************************************************************************

#ifndef COM_MJ_H
#define COM_MJ_H

#include "comexe/ComTdb.h"

// forward declarations

/////////////////////////////////////////////
// class ComTdbMj: Task Definition Block
/////////////////////////////////////////////

class ComTdbMj : public ComTdb {
  friend class ex_mj_tcb;
  friend class ex_mj_unique_tcb;
  friend class ex_mj_private_state;

  enum join_flags {
    SEMI_JOIN = 0x0001,
    LEFT_JOIN = 0x0002,
    ANTI_JOIN = 0x0004,
    LEFT_UNIQUE = 0x0008,
    RIGHT_UNIQUE = 0x0010,
    ENCODED_KEY_COMP_OPT = 0x0020,
    OVERFLOW_ENABLED = 0x0040,
    LOG_DIAGNOSTICS = 0x0080,
    YIELD_QUOTA = 0x0100
  };

 protected:
  ExCriDescPtr workCriDesc_;       // 00-07
  ExExprPtr mergeExpr_;            // 08-15
  ExExprPtr compExpr_;             // 16-23
  ExExprPtr preJoinExpr_;          // 24-31
  ExExprPtr postJoinExpr_;         // 32-39
  ComTdbPtr tdbLeft_;              // 40-47
  ComTdbPtr tdbRight_;             // 48-55
  ExExprPtr leftCheckDupExpr_;     // 56-63
  ExExprPtr rightCheckDupExpr_;    // 64-71
  ExExprPtr ljExpr_;               // 72-79
  ExExprPtr niExpr_;               // 80-87
  ExExprPtr rightCopyDupExpr_;     // 88-95
  int rightDupRecLen_;             // 96-99
  int ljRecLen_;                   // 100-103
  Int16 instantiatedRowAtpIndex_;  // 104-105
  UInt16 flags_;                   // 106-107
  UInt32 encodedKeyLen_;           // 108-111
  Int16 encodedKeyWorkAtpIndex_;   // 112-113
  UInt16 scratchThresholdPct_;     // 114-115
  UInt16 quotaMB_;                 // 116-117
  UInt16 quotaPct_;                // 118-119
  char fillersComTdbMj_[32];       // 120-151

  inline int isSemiJoin() const  // True if doing a semi/anti-semi-join
  {
    return (flags_ & SEMI_JOIN);
  };

  inline int isLeftJoin() const  // True if we are doing a left-join
  {
    return (flags_ & LEFT_JOIN);
  };

  inline int isAntiJoin() const  // True if we are doing an anti-join
  {
    return (flags_ & ANTI_JOIN);
  };

  ex_expr *mergeExpr() { return (encodedKeyCompOpt() ? NULL : mergeExpr_); }
  ex_expr *compExpr() { return (encodedKeyCompOpt() ? NULL : compExpr_); }

  // the next 2 are valid only if ENCODED_KEY_COMP_OPT is set.
  // If set, mergeExpr and CompExpr are used for expressions which
  // create encoded left and right keys so they could be compared.
  ex_expr *leftEncodedKeyExpr() { return (encodedKeyCompOpt() ? mergeExpr_ : NULL); }
  ex_expr *rightEncodedKeyExpr() { return (encodedKeyCompOpt() ? compExpr_ : NULL); }

 public:
  // Constructor
  ComTdbMj();

  ComTdbMj(ComTdb *left_tdb, ComTdb *right_tdb, ex_cri_desc *given_cri_desc, ex_cri_desc *returned_cri_desc,
           ex_expr *merge_expr, ex_expr *comp_expr, ex_expr *left_check_dup_expr, ex_expr *right_check_dup_expr,
           ex_expr *lj_expr, ex_expr *ni_expr, ex_expr *right_copy_dup_expr, int right_dup_reclen, int reclen,
           ex_cri_desc *work_cri_desc, short instantiated_row_atp_index, int encoded_key_len,
           short encoded_key_work_atp_index, ex_expr *pre_join_expr, ex_expr *post_join_expr, queue_index down,
           queue_index up, Cardinality estimatedRowCount, int num_buffers, int buffer_size, int semi_join,
           int left_join, int anti_join, NABoolean left_is_unique = FALSE, NABoolean right_is_unique = FALSE,
           bool isOverflowEnabled = false, UInt16 scratchThresholdPct = 10, UInt16 quotaMB = 0, UInt16 quotaPct = 0,
           bool yieldQuota = true);

  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(1, getClassVersionID());
    ComTdb::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(ComTdbMj); }

  Long pack(void *);
  int unpack(void *, void *reallocator);

  void display() const;

  int orderedQueueProtocol() const;

  // True if we are doing a merge join where the left child returns
  // unique rows (no duplicates)
  inline bool isLeftUnique() const

  {
    return ((flags_ & LEFT_UNIQUE) != 0);
  };

  // True if we are doing a merge join where the right child returns
  // unique rows (no duplicates)

  inline bool isRightUnique() const { return ((flags_ & RIGHT_UNIQUE) != 0); };

  NABoolean encodedKeyCompOpt() const { return ((flags_ & ENCODED_KEY_COMP_OPT) != 0); }

  bool isOverflowEnabled() const { return ((flags_ & OVERFLOW_ENABLED) != 0); }

  // ---------------------------------------------------------------------
  // Used by the internal SHOWPLAN command to get attributes of a TDB.
  // ---------------------------------------------------------------------
  virtual void displayContents(Space *space, int flag);

  // GUI functions
  virtual const ComTdb *getChild(int pos) const {
    if (pos == 0)
      return tdbLeft_;
    else if (pos == 1)
      return tdbRight_;
    else
      return NULL;
  }

  virtual int numChildren() const { return 2; }
  virtual const char *getNodeName() const { return "EX_MJ"; };
  virtual int numExpressions() const { return 9; }
  virtual ex_expr *getExpressionNode(int pos) {
    switch (pos) {
      case 0:
        return mergeExpr_;
      case 1:
        return compExpr_;
      case 2:
        return preJoinExpr_;
      case 3:
        return postJoinExpr_;
      case 4:
        return leftCheckDupExpr_;
      case 5:
        return rightCheckDupExpr_;
      case 6:
        return ljExpr_;
      case 7:
        return niExpr_;
      case 8:
        return rightCopyDupExpr_;
      default:
        return NULL;
    }
  }

  virtual const char *getExpressionName(int pos) const {
    switch (pos) {
      case 0:
        return (encodedKeyCompOpt() ? "leftEncodedKeyExpr_" : "mergeExpr_");
      case 1:
        return (encodedKeyCompOpt() ? "rightEncodedKeyExpr_" : "compExpr_");
      case 2:
        return "preJoinExpr_";
      case 3:
        return "postJoinExpr_";
      case 4:
        return "leftCheckDupExpr_";
      case 5:
        return "rightCheckDupExpr_";
      case 6:
        return "ljExpr_";
      case 7:
        return "niExpr_";
      case 8:
        return "rightCopyDupExpr_";
      default:
        return NULL;
    }
  }

  bool getLogDiagnostics() const { return (flags_ & LOG_DIAGNOSTICS) != 0; }
  void setLogDiagnostics(bool v) { (v ? flags_ |= LOG_DIAGNOSTICS : flags_ &= ~LOG_DIAGNOSTICS); }

  bool getYieldQuota() const { return (flags_ & YIELD_QUOTA) != 0; }
  void setYieldQuota(bool v) { (v ? flags_ |= YIELD_QUOTA : flags_ &= ~YIELD_QUOTA); }

  UInt16 getScratchThresholdPct() const { return scratchThresholdPct_; }
  UInt16 getQuotaMB() const { return quotaMB_; }
  UInt16 getQuotaPct() const { return quotaPct_; }
};

#endif
