
/* -*-C++-*-
****************************************************************************
*
* File:         ComTdbTimeout.h
* Description:  TDB for SET TIMEOUT
*
* Created:      12/27/99
* Language:     C++
*
*
****************************************************************************
*/

#ifndef COM_TDB_TIMEOUT_H
#define COM_TDB_TIMEOUT_H

#include "comexe/ComTdb.h"

///////////////////////////////////////////////////////
//   class ComTdbTimeout
///////////////////////////////////////////////////////
class ComTdbTimeout : public ComTdb {
  friend class ExTimeoutTcb;
  friend class ExTimeoutPrivateState;

 public:
  enum { STO_STREAM = 0x0001, STO_RESET = 0x0002 };

  ComTdbTimeout() : ComTdb(ComTdb::ex_SET_TIMEOUT, eye_SET_TIMEOUT){};

  ComTdbTimeout(ex_expr *timeout_value_expr, ex_cri_desc *work_cri_desc, ex_cri_desc *given_cri_desc,
                ex_cri_desc *returned_cri_desc, queue_index down, queue_index up, int num_buffers, int buffer_size);

  // methods to set/check the RESET / STREAM flags
  void setStream(NABoolean isStream) {
    if (isStream)
      flags_ |= STO_STREAM;
    else
      flags_ &= ~STO_STREAM;
  }

  NABoolean isStream() { return flags_ & STO_STREAM; }

  void setReset(NABoolean isReset) {
    if (isReset)
      flags_ |= STO_RESET;
    else
      flags_ &= ~STO_RESET;
  }

  NABoolean isReset() { return flags_ & STO_RESET; }

  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(1, getClassVersionID());
    ComTdb::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(ComTdbTimeout); }

  Long pack(void *);
  int unpack(void *, void *reallocator);

  int orderedQueueProtocol() const { return -1; };

  virtual const ComTdb *getChild(int /*child*/) const { return NULL; };
  virtual int numChildren() const { return 0; };
  virtual const char *getNodeName() const { return "EX_SET_TIMEOUT"; };
  virtual int numExpressions() const { return 1; };
  virtual const char *getExpressionName(int pos) const { return pos == 0 ? "timeoutValueExpr_" : NULL; };
  virtual ex_expr *getExpressionNode(int pos) { return pos == 0 ? timeoutValueExpr_ : (ExExprPtr)NULL; };

 protected:
  // expression used to compute the timeout value
  ExExprPtr timeoutValueExpr_;  // 00-07

  // cri desc to evaluate the timeoutValueExpr_
  ExCriDescPtr workCriDesc_;  // 08-15

  // Keep the booleans: Is it a stream, is it a RESET
  UInt16 flags_;  // 16-17

  char fillersComTdbTimeout_[46];  // 18-63
};

#endif
