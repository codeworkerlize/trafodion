

#define SQLPARSERGLOBALS_NADEFAULTS  // first

#include "common/ComSchemaName.h"

#include <string.h>

#include "common/ComASSERT.h"
#include "common/ComMPLoc.h"
#include "common/ComSqlText.h"
#include "common/NAString.h"
#include "parser/SqlParserGlobals.h"  // last

//
// constructors
//

//
// default constructor
//
ComSchemaName::ComSchemaName() {}

//
// initializing constructor
//
ComSchemaName::ComSchemaName(const NAString &externalSchemaName) { scan(externalSchemaName); }

//
// initializing constructor
//
ComSchemaName::ComSchemaName(const NAString &externalSchemaName, size_t &bytesScanned) {
  scan(externalSchemaName, bytesScanned);
}

//
// initializing constructor
//
ComSchemaName::ComSchemaName(const ComAnsiNamePart &schemaNamePart) : schemaNamePart_(schemaNamePart) {}

//
// initializing constructor
//
ComSchemaName::ComSchemaName(const ComAnsiNamePart &catalogNamePart, const ComAnsiNamePart &schemaNamePart)
    : catalogNamePart_(catalogNamePart), schemaNamePart_(schemaNamePart) {
  // "cat." is invalid
  if (NOT catalogNamePart_.isEmpty() AND schemaNamePart_.isEmpty()) clear();
}

//
// virtual destructor
//
ComSchemaName::~ComSchemaName() {}

//
// assignment operator
//
ComSchemaName &ComSchemaName::operator=(const NAString &rhsSchemaName) {
  clear();
  scan(rhsSchemaName);
  return *this;
}

//
// accessors
//
const NAString &ComSchemaName::getCatalogNamePartAsAnsiString(NABoolean) const {
  return catalogNamePart_.getExternalName();
}

const NAString &ComSchemaName::getSchemaNamePartAsAnsiString(NABoolean) const {
  return schemaNamePart_.getExternalName();
}

NAString ComSchemaName::getExternalName(NABoolean) const {
  NAString extSchemaName;
#ifndef NDEBUG
  int ok = 0;
#endif

  if (NOT schemaNamePart_.isEmpty()) {
    if (NOT catalogNamePart_.isEmpty()) {
#ifndef NDEBUG
      ok = 1;
#endif
      extSchemaName = getCatalogNamePartAsAnsiString() + "." + getSchemaNamePartAsAnsiString();
    } else {
      extSchemaName = getSchemaNamePartAsAnsiString();
    }
  }

#ifndef NDEBUG
  if (!ok) cerr << "Warning: incomplete ComSchemaName " << extSchemaName << endl;
#endif
  return extSchemaName;
}

//
// mutators
//

//
// Resets data members
//
void ComSchemaName::clear() {
  catalogNamePart_.clear();
  schemaNamePart_.clear();
}

//
//  private methods
//
//
// Scans (parses) input external-format schema name.
//
NABoolean ComSchemaName::scan(const NAString &externalSchemaName) {
  size_t bytesScanned;
  return scan(externalSchemaName, bytesScanned);
}

//
// Scans (parses) input external-format schema name.
//
// This method assumes that the parameter  externalSchemaName  only
// contains the external-format schema name.  The syntax of an
// schema name is
//
//   [ <catalog-name-part> ] . <schema-name-part>
//
// A schema name part must be specified; the catalog name part is optional.
//
// The method returns the number of bytes scanned via the parameter
// bytesScanned.  If the scanned schema name is illegal, bytesScanned
// contains the number of bytes examined when the name is determined
// to be invalid.
//
// If the specified external-format schema name is valid, this method
// returns TRUE and saves the parsed ANSI SQL name part into data
// members catalogNamePart_ and schemaNamePart_; otherwise, it returns
// FALSE and does not changes the contents of the data members.
//
NABoolean ComSchemaName::scan(const NAString &externalSchemaName, size_t &bytesScanned) {
  size_t count;
  size_t externalSchemaNameLen = externalSchemaName.length();
  bytesScanned = 0;

  // Each ComAnsiNamePart ctor below must be preceded by "count = 0;"
  // -- see ComAnsiNamePart.cpp, and for a better scan implementation,
  //    see ComObjectName::scan() + ComObjectName(bytesScanned) ctor.

  // ---------------------------------------------------------------------
  // Scan the leftmost ANSI SQL name part.
  // ---------------------------------------------------------------------

  count = 0;
  ComAnsiNamePart part1(externalSchemaName, count);
  bytesScanned += count;
  if (NOT part1.isValid()) return FALSE;

  if (bytesScanned >= externalSchemaNameLen) {
    ComASSERT(bytesScanned == externalSchemaNameLen);
    schemaNamePart_ = part1;
    return TRUE;  // "sch"
  }

  // Get past the period separator
  if (NOT ComSqlText.isPeriod(externalSchemaName[bytesScanned++])) return FALSE;

  // ---------------------------------------------------------------------
  // Scan the last ANSI SQL name part
  // ---------------------------------------------------------------------

  int remainingLen = externalSchemaNameLen - bytesScanned;
  NAString remainingName = externalSchemaName(bytesScanned, remainingLen);
  count = 0;
  ComAnsiNamePart part2(remainingName, count);
  bytesScanned += count;
  if (NOT part2.isValid()) return FALSE;

  if (bytesScanned == externalSchemaNameLen) {
    catalogNamePart_ = part1;
    schemaNamePart_ = part2;
    return TRUE;  // "cat.sch"
  }

  // The specified external-format object name contains some extra
  // trailing characters -- illegal.
  //
  return FALSE;

}  // ComSchemaName::scan()

void ComSchemaName::setDefinitionSchemaName(const COM_VERSION version) {}
