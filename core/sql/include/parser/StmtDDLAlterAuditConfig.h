#ifndef STMTDDLALTERAUDITCONFIG_H
#define STMTDDLALTERAUDITCONFIG_H
//******************************************************************************

//******************************************************************************
/*
********************************************************************************
*
* File:         StmtDDLAlterAuditConfig.h
* Description:  class for parse node representing alter audit config statements
*
*
* Created:      8/21/2012
* Language:     C++
*
*
*
*
********************************************************************************
*/
#include "common/ComSmallDefs.h"
#include "StmtDDLNode.h"

class StmtDDLAlterAuditConfig : public StmtDDLNode

{
 public:
  StmtDDLAlterAuditConfig();

  StmtDDLAlterAuditConfig(const NAString &logName, const NAString &columns, const NAString &values);

  virtual ~StmtDDLAlterAuditConfig();

  virtual StmtDDLAlterAuditConfig *castToStmtDDLAlterAuditConfig();

  //
  // method for binding
  //

  ExprNode *bindNode(BindWA *bindWAPtr);

  // accessors

  inline const NAString getLogName() const;
  inline const NAString getColumns() const;
  inline const NAString getValues() const;

  // for tracing

  virtual const NAString displayLabel1() const;
  virtual const NAString displayLabel2() const;
  virtual const NAString getText() const;

 private:
  const NAString &logName_;
  const NAString &columns_;
  const NAString &values_;
};

//----------------------------------------------------------------------------
// definitions of inline methods for class StmtDDLAlterAuditConfig
//----------------------------------------------------------------------------

//
// accessors
//

inline const NAString StmtDDLAlterAuditConfig::getLogName() const { return logName_; }

inline const NAString StmtDDLAlterAuditConfig::getColumns() const { return columns_; }

inline const NAString StmtDDLAlterAuditConfig::getValues() const { return values_; }

#endif  // STMTDDLALTERAUDITCONFIG
