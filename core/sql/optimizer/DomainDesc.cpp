/* -*-C++-*-
******************************************************************************
*
* File:         DomainDesc.C
* Description:  A Domain descriptor
*
* Created:      12/8/1994
* Language:     C++
*

*
******************************************************************************
*/

#include "optimizer/Sqlcomp.h"

// -----------------------------------------------------------------------
// Constructor
// -----------------------------------------------------------------------
DomainDesc::DomainDesc(SchemaDB *schemaDB, const NAType &refToType) : type_(&refToType) {
  CMPASSERT(&refToType);
  schemaDB->insertDomainDesc(this);
}
