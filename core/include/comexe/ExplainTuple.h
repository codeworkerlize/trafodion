
/* -*-C++-*-
****************************************************************************
*
* File:         ExplainTuple.h
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

#ifndef EXPLAINTUPLE_H
#define EXPLAINTUPLE_H

#include "common/CharType.h"
#include "common/Collections.h"
#include "common/Int64.h"
#include "common/charinfo.h"
#include "export/NAVersionedObject.h"

const int MAX_REL_ARITYS = 2;

class ExplainTuple;
class ExplainDesc;

// Structure used to hold column information for the Virtual
// Explain Table.  An array of these will be constructed by the
// ExplainDesc constructor.  This array will be pointed to by
// the root ExplainDesc node of the explain Tree.

struct ExplainTupleColDesc {
  // type should correspond to DataType in desc.h
  int datatype;                      // 00-03
  int length;                        // 04-07
  int offset;                        // 08-11
  Int16 nullflag;                    // 12-13
  Int16 fillersExplainTupleColDesc;  // 14-15
};

// ---------------------------------------------------------------------
// Template instantiation to produce a 64-bit pointer emulator class
// for ExplainTuple
// ---------------------------------------------------------------------
typedef NAVersionedObjectPtrTempl<ExplainTuple> ExplainTuplePtr;

// ---------------------------------------------------------------------
// Template instantiation to produce a 64-bit pointer emulator class
// for ExplainDesc
// ---------------------------------------------------------------------
typedef NAVersionedObjectPtrTempl<ExplainDesc> ExplainDescPtr;

// ---------------------------------------------------------------------
// Template instantiation to produce a 64-bit pointer emulator class
// for ExplainTupleColDescPtr
// ---------------------------------------------------------------------
typedef NABasicPtrTempl<ExplainTupleColDesc> ExplainTupleColDescPtr;

// ExplainDesc:  This class implements the root node of the explain
// tree.  The node contains:
//  - a pointer to the rest of the explain tree
//    (made up of ExplainTuple nodes)
//  - a pointer to an ExplainTupleColDesc[].  This is a descriptor
//    of the explainTuple.
//
class ExplainDesc : public NAVersionedObject {
 public:
  ExplainDesc() : NAVersionedObject(-1) {}

  // The constructor, Allocates the ExplainTupleColDesc array.
  ExplainDesc(int numCols, int recLength, Space *space);

  // The destuctor (none currently defined.  This node should be
  // allocated using a Space object, so will be reclaimed in bulk)
  ~ExplainDesc() {}

  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() { setImageVersionID(0, getClassVersionID()); }

  virtual short getClassSize() { return (short)sizeof(ExplainDesc); }

  // Pack and Unpack the ExplainDesc node and its children
  Long pack(void *);
  int unpack(void *, void *reallocator);

  // set and get a pointer to the rest of the explain tree.
  inline void setExplainTreeRoot(ExplainTuple *rootExplainTuple);
  inline ExplainTuple *getExplainTreeRoot();

  // set the column descriptions
  inline void setColDescr(int col, int datatype, int length, int offset, int nullflag);

  // Get values of the various fields of the class
  inline int getRecLength();
  inline int getNumCols();
  inline int getColLength(int col);
  inline int getColOffset(int col);
  inline short getColNullFlag(int col);
  inline int getColDataType(int col);

 private:
  // The version of the explain tree.  Might be useful to handle
  // versioning issues.
  // int version_;
  // (Handled in NAVersionedObject header now.)

  // Number of columns in the explainTuple.
  int numCols_;  // 00-03

  // Number of bytes in the explainTuple.
  int recLength_;  // 04-07

  // A pointer to an ExplainTupleColDesc array.
  ExplainTupleColDescPtr explainCols_;  // 08-15

  // Pointer to the rest of the explain tree.
  ExplainTuplePtr explainTreeRoot_;  // 16-23

  char fillersExplainDesc_[40];  // 24-63
};

inline void ExplainDesc::setExplainTreeRoot(ExplainTuple *rootExplainTuple) { explainTreeRoot_ = rootExplainTuple; };

inline ExplainTuple *ExplainDesc::getExplainTreeRoot() { return explainTreeRoot_; };

inline void ExplainDesc::setColDescr(int col, int datatype, int length, int offset, int nullflag) {
  explainCols_[col].datatype = datatype;
  explainCols_[col].length = length;
  explainCols_[col].offset = offset;
  explainCols_[col].nullflag = (Int16)nullflag;
}

inline int ExplainDesc::getRecLength() { return recLength_; };

inline int ExplainDesc::getNumCols() { return numCols_; };

inline int ExplainDesc::getColLength(int col) { return explainCols_[col].length; };

inline int ExplainDesc::getColOffset(int col) { return explainCols_[col].offset; };

inline short ExplainDesc::getColNullFlag(int col) { return explainCols_[col].nullflag; };

inline int ExplainDesc::getColDataType(int col) { return explainCols_[col].datatype; };

// ExplainTuple - A binary node of the explain tree.  Besides the root
// node (ExplainDesc), the explain tree is made up of these
// exclusively.  The explain tree is generated a generate time in a
// separate fragment by the methods addExplainInfo().  This class
// implements some methods to set certain fields of the explainTuple.
// Only the ones that can be called from within the executor are
// implemented here.  The others are implemented in the class
// ExplainTupleMaster which is derived from this class and only contains
// methods.

class ExplainTuple : public NAVersionedObject {
 public:
  // The enumeration of the columns of the Explain Tuple.
  // The order (and values of the tokens) of this enumeration
  // is important.
  enum ExplainTupleCols {
    EX_COL_SYSKEY,
    EX_COL_MODNAME,
    EX_COL_STMTNAME,
    EX_COL_PLANID,
    EX_COL_SEQNUM,
    EX_COL_OPER,
    EX_COL_LEFTSEQNUM,
    EX_COL_RIGHTSEQNUM,
    EX_COL_TABLENAME,
    EX_COL_CARD,
    EX_COL_OPCOST,
    EX_COL_TOTCOST,
    EX_COL_DETCOST,
    EX_COL_DESCRIPT
  };

  // The states of the explain Tuple.  If no info has been
  // placed into the tuple, it should not be processed.
  enum ExplainTupleState { NO_EXPLAIN_INFO, SOME_EXPLAIN_INFO };

  ExplainTuple() : NAVersionedObject(-1), explainTupleStr_(NULL) {}

  // Contructor - called by RelExpr::addExplainInfo()
  ExplainTuple(ExplainTuple *leftChild, ExplainTuple *rightChild, ExplainDesc *explainDesc);

  ~ExplainTuple();

  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() { setImageVersionID(0, getClassVersionID()); }

  virtual short getClassSize() { return (short)sizeof(ExplainTuple); }

  // Pack and Unpack this node and all its children
  Long pack(void *);
  int unpack(void *, void *reallocator);

  short genExplainTupleData(Space *space);

  // Copy the data pointed to by value to the specified col of the
  // explain tuple.
  void setCol(int col, void const *value, UInt32 length, UInt32 cursor);

  // Initialize specific columns of the explain Tuple.
  // These routines call setCol().
  // These are the columns that need to be set at run time.
  // The methods to set the other columns are implemented in
  // the class ExplainTupleMaster.
  void setModuleName(const char *modName);
  void setStatementName(const char *stmtName);
  void setSeqNum(int seqNum);
  int getSeqNum();
  void setChildSeqNum(int child, int seqNum);

  // The state of this explain tuple.
  inline ExplainTupleState getState() const;

  // get/set the pointer to the parent of this node.
  // The child of the ExplainDesc node will have a NULL parent
  // pointer.
  inline void setParent(ExplainTuple *parent);
  inline ExplainTuple *getParent();

  // The number of children this node can have. (always returns 2)
  inline int numChildren() const;

  // A reference to a child.
  inline ExplainTuple *&child(int index);

  // A pointer to the explainTuple data;
  inline char *getExplainTuple();

  // A pointer to the root node (ExplainDesc) of the explain tree.
  // This node contains the description of the explain tuple.
  inline ExplainDesc *getExplainDesc();

  // Get info about the explain tuple.  These methods access the
  // ExplainDesc node at the root of the tree.
  inline int getRecLength();
  inline int getNumCols();
  inline int getColLength(int col);
  inline int getColOffset(int col);
  inline short getColNullFlag(int col);
  inline int getColDataType(int col);

  int getUsedRecLength() { return usedRecLength_; }
  void setUsedRecLength(int v) { usedRecLength_ = v; }

 protected:
  // Used for debugging and self checks.
  char eyeCatcher_[4];  // 00-03

  // State of this explain Tuple. (ExplainTupleState)
  Int16 state_;                 // 04-05
  Int16 fillersExplainTuple1_;  // 06-07

  // A cursor for the description field of the explain Tuple.
  // The description field is filled in in peices.  The cursor
  // is used to keep track of the current position.
  UInt32 descCursor_;         // 08-11
  int fillersExplainTuple2_;  // 12-15

  // A pointer to the data.
  NABasicPtr explainTupleData_;  // 16-23

  // A pointer back to the root of the explain tree.
  ExplainDescPtr explainDesc_;  // 24-31

  // Pointer to the parent node.
  ExplainTuplePtr parent_;  // 32-39

  // Pointers to the children nodes.
  ExplainTuplePtr children_[MAX_REL_ARITYS];  // 40-55

  // this field is initially allocated from heap for max length of explain tuple.
  // At end of explain, only the used up bytes are moved to explainTupleData
  // to be stored in explain fragment.
  // This is done to reduce the amount of data in explain fragment since that
  // could be stored in trafodion repository.
  char *explainTupleStr_;

  // Number of actual bytes of data in explainTuple.
  int usedRecLength_;

  char fillersExplainTuple_[36];  // 56-95
};

// Inline routines for ExplainTuple.

inline ExplainTuple::ExplainTupleState ExplainTuple::getState() const { return ExplainTupleState(state_); };

inline int ExplainTuple::numChildren() const { return MAX_REL_ARITYS; };

inline ExplainTuple *&ExplainTuple::child(int index) { return children_[index].pointer(); };

inline void ExplainTuple::setParent(ExplainTuple *parent) { parent_ = parent; };

inline ExplainTuple *ExplainTuple::getParent() { return parent_; };

inline char *ExplainTuple::getExplainTuple() { return explainTupleData_; };

inline ExplainDesc *ExplainTuple::getExplainDesc() { return explainDesc_; };

inline int ExplainTuple::getRecLength() { return explainDesc_->getRecLength(); };

inline int ExplainTuple::getNumCols() { return explainDesc_->getNumCols(); };

inline int ExplainTuple::getColLength(int col) { return explainDesc_->getColLength(col); };

inline int ExplainTuple::getColOffset(int col) { return explainDesc_->getColOffset(col); };

inline short ExplainTuple::getColNullFlag(int col) { return explainDesc_->getColNullFlag(col); };

inline int ExplainTuple::getColDataType(int col) { return explainDesc_->getColDataType(col); };

#endif
