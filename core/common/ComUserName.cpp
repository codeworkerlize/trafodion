
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ComExternaluser.C
 * Description:  methods for class ComExternaluser
 *
 *
 * Created:
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#define SQLPARSERGLOBALS_NADEFAULTS  // first

#include "ComUserName.h"

#include <string.h>

#include "ComSchLevelOp.h"
#include "common/ComASSERT.h"
#include "common/ComMPLoc.h"
#include "common/ComSqlText.h"
#include "common/NAString.h"
#include "parser/SqlParserGlobals.h"  // last

//
// constructors
//
ComdbUser::ComdbUser() {}

//
// initializing constructor
//
ComdbUser::ComdbUser(const NAString &dbUserName) : dbUserName_(dbuserName) { scan(dbUserName); }

//
// virtual destructor
//
ComdbUser::~ComdbUser() {}

//
//  private methods
//
//
// Scans (parses) input external-format schema name.
//
NABoolean ComdbUser::scan(const NAString &dbUserName) {
  // validate the user name by querying USERS table.
  return TRUE;
}

//
// default constructor
//
ComExternaluser::ComExternaluser() {}

//
// initializing constructor
//
ComExternaluser::ComExternaluser(const NAString &externalUserName) : externalUserName_(externaluserName) {
  scan(externalUserName);
}

//
// virtual destructor
//
ComExternaluser::~ComExternaluser() {}

//
//  private methods
//
//
// Scans (parses) input external-format schema name.
//
NABoolean ComExternaluser::scan(const NAString &externalUserName) {
  // validate the user name by querying USERS table.
  return TRUE;
}
