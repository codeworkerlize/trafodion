
/* -*-C++-*-
****************************************************************************
*
* File:         ComTdbOnlj.h
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

#ifndef COM_ONLJ_H
#define COM_ONLJ_H

// An onlj is a nested loop join that follows the ordered queue protocol
#include "comexe/ComTdb.h"

//
// Task Definition Block
//
class ComTdbOnlj : public ComTdb {
  friend class ExOnljTcb;
  friend class ExOnljPrivateState;

  enum join_flags {
    SEMI_JOIN = 0x0001,
    LEFT_JOIN = 0x0002,
    ANTI_JOIN = 0x0004,
    ROWSET_ITERATOR = 0x0008,
    VSBB_INSERT = 0x0010,
    INDEX_JOIN = 0x0020,
    UNDO_JOIN = 0x0040,
    SET_NONFATAL_ERROR = 0x0080,
    DRIVING_MV_LOGGING = 0x0100
  };

 protected:
  ComTdbPtr tdbLeft_;               // 00-07
  ComTdbPtr tdbRight_;              // 08-15
  ExCriDescPtr workCriDesc_;        // 16-23
  ExExprPtr preJoinPred_;           // 24-31
  ExExprPtr postJoinPred_;          // 32-39
  ExExprPtr ljExpr_;                // 40-47
  ExExprPtr niExpr_;                // 48-55
  int ljRecLen_;                  // 56-59
  UInt16 instantiatedRowAtpIndex_;  // 60-61
  UInt16 flags_;                    // 62-63
  int rowsetRowCountArraySize_;   // 64-67
  char fillersComTdbOnlj_[36];      // 68-103

  inline int isSemiJoin() const;        // True if we are doing a semi-join
  inline int isAntiJoin() const;        // True if we are doing a anti-join
  inline int isLeftJoin() const;        // True if we are doing a left-join
  inline int isUndoJoin() const;        // True if we are using this to drive an undo
                                          // tree
  inline int isSetNFErrorJoin() const;  // True if we are using this to set the NF row indexes
                                          // tree
  inline int isRowsetIterator() const;  // True if we are using this onlj to flow entries in a rowset
  inline int isIndexJoin() const;       // True if this onlj is an index join
  inline NABoolean vsbbInsertOn() const;
  inline NABoolean isDrivingMVLogging() const;  // True if this onlj is used to drive mv logging
  // returns positive value only if this onlj is being used for rowset update and deletes
  // and rowset_row_count feature is enabled.
  inline int getRowsetRowCountArraySize() const;

 public:
  // Constructor
  ComTdbOnlj();

  ComTdbOnlj(ComTdb *left_tdb, ComTdb *right_tdb, ex_cri_desc *given_cri_desc, ex_cri_desc *returned_cri_desc,
             queue_index down, queue_index up, Cardinality estimatedRowCount, int num_buffers, int buffer_size,
             ex_expr *before_pred, ex_expr *after_pred, ex_expr *lj_expr, ex_expr *ni_expr, ex_cri_desc *work_cri_desc,
             const unsigned short instantiated_row_atp_index, int reclen, int semi_join, int anti_semi_join,
             int left_join, int undo_join, int setNFError, int rowset_iterator, int index_join,
             NABoolean vsbbInsert, int rowsetRowCountArraySize, NABoolean tolerateNonFatalError,
             NABoolean drivingMVLogging);

  virtual ~ComTdbOnlj();

  int orderedQueueProtocol() const;

  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(1, getClassVersionID());
    ComTdb::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(ComTdbOnlj); }

  Long pack(void *);
  int unpack(void *, void *reallocator);

  void display() const;

  inline ComTdb *getLeftTdb();
  inline ComTdb *getRightTdb();
  inline void setLeftTdb(ComTdb *);
  inline void setRightTdb(ComTdb *);

  virtual const ComTdb *getChild(int pos) const;
  virtual int numChildren() const { return 2; }
  virtual const char *getNodeName() const;
  virtual int numExpressions() const { return 4; }
  virtual ex_expr *getExpressionNode(int pos);
  virtual const char *getExpressionName(int pos) const;

  // ---------------------------------------------------------------------
  // Used by the internal SHOWPLAN command to get attributes of a TDB.
  // ---------------------------------------------------------------------
  virtual void displayContents(Space *space, int flag);
};

inline ComTdb *ComTdbOnlj::getLeftTdb() { return tdbLeft_; };
inline ComTdb *ComTdbOnlj::getRightTdb() { return tdbRight_; };
inline void ComTdbOnlj::setLeftTdb(ComTdb *left) { tdbLeft_ = left; };
inline void ComTdbOnlj::setRightTdb(ComTdb *right) { tdbRight_ = right; };

inline int ComTdbOnlj::orderedQueueProtocol() const {
  return -1;  // return true
};

// tdb inline procedures
// to determine if it is a semi_join or a left join check the flags_

inline int ComTdbOnlj::isSemiJoin() const { return (flags_ & SEMI_JOIN); };

inline int ComTdbOnlj::isAntiJoin() const { return (flags_ & ANTI_JOIN); };

inline int ComTdbOnlj::isLeftJoin() const { return (flags_ & LEFT_JOIN); };

inline int ComTdbOnlj::isUndoJoin() const { return (flags_ & UNDO_JOIN); };
inline int ComTdbOnlj::isSetNFErrorJoin() const { return (flags_ & SET_NONFATAL_ERROR); };
inline int ComTdbOnlj::isRowsetIterator() const { return (flags_ & ROWSET_ITERATOR); };

inline int ComTdbOnlj::isIndexJoin() const { return (flags_ & INDEX_JOIN); };

inline NABoolean ComTdbOnlj::vsbbInsertOn() const { return (flags_ & VSBB_INSERT); }
inline NABoolean ComTdbOnlj::isDrivingMVLogging() const { return (flags_ & DRIVING_MV_LOGGING); }

inline int ComTdbOnlj::getRowsetRowCountArraySize() const { return rowsetRowCountArraySize_; }

// end of inline procedures

#endif  // EX_ONLJ_H
