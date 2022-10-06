
#ifndef HVROLESASSIGN_H
#define HVROLESASSIGN_H

#include "parser/HvRoles.h"

// These two static functions were moved from HvRoles.h to this file as
// they are called only by sqlparser.y while HvRoles.h is included by many
// other cpp files. This was causing the compiler to emit warning 262  (function
// defined but not referenced). Therefore the definitions were moved to this file
// this file will be included by sqlparser.y. Warning Elimination effort 11/01/2003

/* This function, assignRole, is designed as a shorthand for the few lines
 * of code which it contains.  This code is used over and over in the
 * action code for the right sides of many of the productions which are for
 * SQL statements that involves host variables.
 *
 * The purpose of this function is to allow the client code to pass in a pointer
 * and if the pointer is NULL, only then should this function assign the given
 * role to the first Unassigned host variable in TheHostVarRoles.
 */

static void assignRoleToFirst(void *ptrAsFlag, HostVarRole theRole) {
  assert(theRole != HV_UNASSIGNED && theRole);
  if (ptrAsFlag == NULL)
    // Then entity_name is a host variable per theRole
    TheHostVarRoles->setFirstUnassignedTo(theRole);
  // if
}

static void assignRoleToLast(void *ptrAsFlag, HostVarRole theRole) {
  assert(theRole != HV_UNASSIGNED && theRole);
  if (ptrAsFlag == NULL)
    // Then entity_name is a host variable per theRole
    TheHostVarRoles->setLastUnassignedTo(theRole);
  // if
}

#endif  // HVROLES_H
