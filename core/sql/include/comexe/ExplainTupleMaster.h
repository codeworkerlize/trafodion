/* -*-C++-*-
****************************************************************************
*
* File:         ExplainTupleMaster.h
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

#ifndef EX_EXPLAINTUPLEM_H
#define EX_EXPLAINTUPLEM_H

#include "exp/ExpSqlTupp.h"
#include "common/Int64.h"
#include "comexe/ExplainTuple.h"

class Cost;

class ExplainTupleMaster : public ExplainTuple {
 public:
  inline ExplainTupleMaster(ExplainTuple *leftChild, ExplainTuple *rightChild, ExplainDesc *explainDesc);
  ~ExplainTupleMaster(){};

  int init(Space *space, NABoolean doExplainSpaceOpt);
  void setPlanId(long planId);
  void setOperator(const char *op);
  void setTableName(const char *tabName);
  void setCardinality(double card);
  void setOperatorCost(double opCost);
  void setTotalCost(double totCost);
  void setDetailCost(char *detail);
  void setDescription(const char *desc);

 private:
};

inline ExplainTupleMaster::ExplainTupleMaster(ExplainTuple *leftChild, ExplainTuple *rightChild,
                                              ExplainDesc *explainDesc)
    : ExplainTuple(leftChild, rightChild, explainDesc){};

#endif
