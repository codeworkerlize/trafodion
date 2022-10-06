

#ifndef COM_STORED_PROC_H
#define COM_STORED_PROC_H

#include "comexe/ComTdb.h"
#include "exp/exp_tuple_desc.h"
#include "exp/exp_clause_derived.h"
#include "optimizer/RelStoredProc.h"
class ExpTupleDesc;
class ComDiagsArea;

/////////////////////////////////////////////////////////////////
// This class is used to extract fields out of the
// input row and move fields to the output row.
// Used by procs ExSPExtractInputValue and ExSPMoveOutputValue.
/////////////////////////////////////////////////////////////////
class ExSPInputOutput : public NAVersionedObject {
 public:
  ExSPInputOutput();

  ~ExSPInputOutput(){};

  void initialize(ExpTupleDesc *tupleDesc, int totalLen, ConvInstruction *caseIndexArray);

  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() { setImageVersionID(0, getClassVersionID()); }

  virtual short getClassSize() { return (short)sizeof(ExSPInputOutput); }

  Long pack(void *space);
  int unpack(void *base, void *reallocator);

  short inputValue(int fieldNum, char *inputRow, char *data, int datalen, NABoolean casting,
                   ComDiagsArea *diagsArea = NULL);

  short outputValue(int fieldNum, char *outputRow, char *data, int datalen, NABoolean casting, CollHeap *heap,
                    ComDiagsArea *diagsArea = NULL);

  int getLength() { return totalLen_; };

  ConvInstruction *getCaseIndexArray() { return (ConvInstruction *)(short *)caseIndexArray_; }

 protected:
  // "SPIO"
  char eyeCatcher_[4];  // 00-03

  UInt32 flags_;  // 04-07

  // pointer to the record descriptor
  ExpTupleDescPtr tupleDesc_;  // 08-15

  // this is a (enum ConvInstruction *) before.
  Int16Ptr caseIndexArray_;  // 16-23

  // length of ExSPInputOutput starting at 'this'. All classes following
  // 'this' are allocated contiguosly.
  UInt32 totalLen_;  // 24-27

  char fillersExSPInputOutput_[36];  // 28-63
};

// ---------------------------------------------------------------------
// Template instantiation to produce a 64-bit pointer emulator class
// for ExSPInputOutput
// ---------------------------------------------------------------------
typedef NAVersionedObjectPtrTempl<ExSPInputOutput> ExSPInputOutputPtr;

class ComTdbStoredProc : public ComTdb {
  friend class ExStoredProcTcb;
  friend class ExStoredProcPrivateState;

 public:
  ComTdbStoredProc() : ComTdb(ComTdb::ex_STORED_PROC, eye_STORED_PROC){};

  ComTdbStoredProc(char *spName, ex_expr *inputExpr, int inputRowlen, ex_expr *outputExpr, int outputRowlen,
                   ex_cri_desc *workCriDesc, const unsigned short workAtpIndex, ex_cri_desc *criDescParent,
                   ex_cri_desc *criDescReturned, ExSPInputOutput *extractInputExpr, ExSPInputOutput *moveOutputExpr,
                   queue_index fromParent, queue_index toParent, Cardinality estimatedRowCount, int numBuffers,
                   int bufferSize, ex_expr *predExpr = NULL,
                   UInt16 arkcmpInfo = 0);  // constructor

  ~ComTdbStoredProc();

  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(1, getClassVersionID());
    ComTdb::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(ComTdbStoredProc); }

  Long pack(void *);
  int unpack(void *, void *reallocator);

  int orderedQueueProtocol() const { return -1; };
  void display() const {};

  int numChildren() const { return 0; };
  const ComTdb *getChild(int) const { return NULL; };
  const char *getNodeName() const { return "EX_STORED_PROC"; };
  int numExpressions() const { return 3; };
  virtual ex_expr *getExpressionNode(int pos) {
    if (pos == 0)
      return inputExpr_;
    else if (pos == 1)
      return outputExpr_;
    else if (pos == 2)
      return predExpr_;
    else
      return NULL;
  }

  virtual const char *getExpressionName(int pos) const {
    if (pos == 0)
      return "inputExpr_";
    else if (pos == 1)
      return "outputExpr_";
    else if (pos == 2)
      return "predExpr_";
    else
      return NULL;
  }

  NABasicPtr getSPName() const { return spName_; }

  NABoolean getUseExistingArkcmp() const { return (arkcmpInfo_ & RelInternalSP::executeInSameArkcmp); }

  NABoolean isExecuteInLocalProcess() const { return (arkcmpInfo_ & RelInternalSP::executeInLocalProcess); }

 protected:
  // Name of stored procedure
  NABasicPtr spName_;  // 00-07

  ExCriDescPtr workCriDesc_;  // 08-15

  // describes the input and output rows. Used to extract
  // input values from the input row or move in output values
  // to the output row by SP.
  ExSPInputOutputPtr extractInputExpr_;  // 16-23
  ExSPInputOutputPtr moveOutputExpr_;    // 24-31

  // expression to compute the input data row sent to ARKCMP.
  ExExprPtr inputExpr_;  // 32-39
  UInt32 inputRowlen_;   // 40-43

  UInt32 flags_;  // 44-47

  // expression to compute the output row returned to parent's up queue.
  // The output row is created based on the data returned by ARKCMP.
  ExExprPtr outputExpr_;  // 48-55
  UInt32 outputRowlen_;   // 56-59

  UInt16 workAtpIndex_;  // 60-61

  // Bitmap that contains arkcmp-related information passed from the compiler
  // to the executor. Internal stored procedures use it to indicate that
  // the procedure in execution corresponds to query cache statistics
  UInt16 arkcmpInfo_;  // 62-63
  // expression to evaluate the predicate in the WHERE clause of an
  // internal stored procedure
  ExExprPtr predExpr_;  // 64-71

  char fillersComTdbStoredProc_[32];  // 72-103
};

#endif
