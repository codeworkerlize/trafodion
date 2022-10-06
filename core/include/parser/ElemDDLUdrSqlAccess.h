
#ifndef ELEMDDLUDRSQLACCESS_H
#define ELEMDDLUDRSQLACCESS_H
/* -*-C++-*-
******************************************************************************
*
* File:         ElemDDLUdrSqlAccess.h
* Description:  class forUDR SQL Access (parse node) elements in
*               DDL statements
*
*
* Created:      10/08/1999
* Language:     C++
*
*
*
*
******************************************************************************
*/

#include "ElemDDLNode.h"
#include "common/ComSmallDefs.h"

class ElemDDLUdrSqlAccess : public ElemDDLNode {
 public:
  // default constructor
  ElemDDLUdrSqlAccess(ComRoutineSQLAccess theSqlAccess);

  // virtual destructor
  virtual ~ElemDDLUdrSqlAccess(void);

  // cast
  virtual ElemDDLUdrSqlAccess *castToElemDDLUdrSqlAccess(void);

  // accessor
  inline const ComRoutineSQLAccess getSqlAccess(void) const { return sqlAccess_; }

  //
  // methods for tracing
  //

  virtual NATraceList getDetailInfo() const;
  virtual const NAString getText() const;

 private:
  ComRoutineSQLAccess sqlAccess_;

};  // class ElemDDLUdrSqlAccess

#endif /* ELEMDDLUDRSQLACCESS_H */
