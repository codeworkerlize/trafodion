
/* -*-C++-*-
******************************************************************************
*
* File:         $File$
* RCS:          $Id$
* Description:
* Created:
* Language:     C++
* Status:       $State$
*
*
*
*
******************************************************************************
*/

#ifndef ComTdbSample_h
#define ComTdbSample_h

#include "comexe/ComTdb.h"
#include "comexe/ComPackDefs.h"

// Task Definition Block
//
class ComTdbSample : public ComTdb {
  friend class ExSampleTcb;
  friend class ExSamplePrivateState;

 public:
  ComTdbSample();

  ComTdbSample(ex_expr *initExpr, ex_expr *balanceExpr, int returnFactorOffset, ex_expr *postPred, ComTdb *child_tdb,
               ex_cri_desc *given_cri_desc, ex_cri_desc *returned_cri_desc, queue_index down, queue_index up);

  ~ComTdbSample();

  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(1, getClassVersionID());
    ComTdb::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(*this); }

  Long pack(void *);
  int unpack(void *, void *reallocator);

  void display() const;

  inline ComTdb *getChildTdb();

  int orderedQueueProtocol() const;

  virtual const ComTdb *getChild(int pos) const;
  virtual int numChildren() const { return 1; }
  virtual const char *getNodeName() const { return "EX_SAMPLE"; };
  virtual int numExpressions() const { return 3; }
  virtual ex_expr *getExpressionNode(int pos) {
    if (pos == 0)
      return initExpr_;
    else if (pos == 1)
      return balanceExpr_;
    else if (pos == 2)
      return postPred_;
    else
      return NULL;
  }
  virtual const char *getExpressionName(int pos) const {
    if (pos == 0)
      return "initExpr_";
    else if (pos == 1)
      return "balanceExpr_";
    else if (pos == 2)
      return "postPred_";
    else
      return NULL;
  }

 protected:
  ExExprPtr initExpr_;        // 00-07
  ExExprPtr balanceExpr_;     // 08-15
  ExExprPtr postPred_;        // 16-23
  ComTdbPtr tdbChild_;        // 24-31
  int returnFactorOffset_;  // 32-35
  // ---------------------------------------------------------------------
  // Filler for potential future extensions without changing class size.
  // When a new member is added, size of this filler should be reduced so
  // that the size of the object remains the same (and is modulo 8).
  // ---------------------------------------------------------------------
  char fillers_[36];  // 36-71
};

inline ComTdb *ComTdbSample::getChildTdb() { return tdbChild_; };

/*****************************************************************************
  Description : Return ComTdb* depending on the position argument.
                  Position 0 means the left most child.
  Comments    :
  History     : Yeogirl Yun                                      8/22/95
                 Initial Revision.
*****************************************************************************/
inline const ComTdb *ComTdbSample::getChild(int pos) const {
  if (pos == 0)
    return tdbChild_;
  else
    return NULL;
}

#endif
