#ifndef STMTDDLDROPCOMPONENTPRIVILEGE_H
#define STMTDDLDROPCOMPONENTPRIVILEGE_H

/* -*-C++-*-
******************************************************************************
*
* File:          StmtDDLDropComponentPrivilege.h
* RCS:           $Id:
* Description:   class for parse node representing the
*                "drop component privilege" statement.
*
*
* Dropd:       06/24/2011
* Language:      C++
*
*
*
*
******************************************************************************
*/

#include "common/ComSmallDefs.h"
#include "parser/StmtDDLNode.h"
#define SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#include "parser/SqlParserGlobals.h"

class StmtDDLDropComponentPrivilege : public StmtDDLNode

{
 public:
  // constructor
  StmtDDLDropComponentPrivilege(const NAString &componentPrivilegeName, const NAString &componentName,
                                ComDropBehavior dropBehavior, CollHeap *heap = PARSERHEAP());

  // Virtual Destructor
  virtual ~StmtDDLDropComponentPrivilege();

  // Cast

  virtual StmtDDLDropComponentPrivilege *castToStmtDDLDropComponentPrivilege();

  // accessors

  inline const NAString &getComponentPrivilegeName() const { return componentPrivilegeName_; }
  inline const NAString &getComponentName() const { return componentName_; }
  inline const ComDropBehavior getDropBehavior() const { return dropBehavior_; }

  // for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString displayLabel2() const;
  virtual const NAString getText() const;

 private:
  // not-supported methods

  StmtDDLDropComponentPrivilege();                                                  // DO NOT USE
  StmtDDLDropComponentPrivilege(const StmtDDLDropComponentPrivilege &);             // DO NOT USE
  StmtDDLDropComponentPrivilege &operator=(const StmtDDLDropComponentPrivilege &);  // DO NOT USE

  // Data members

  NAString componentPrivilegeName_;
  NAString componentName_;
  ComDropBehavior dropBehavior_;
};

#endif  // STMTDDLDROPCOMPONENTPRIVILEGE_H
