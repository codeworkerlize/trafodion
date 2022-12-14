
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ComSmallDefs.C
 * Description:  Small definitions are declared here that are used throughout
 *               the SQL/ARK product.
 *
 * Created:      10/27/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "common/Platform.h"

// this file needed for getuid/getpid for UNIX implementation of ComUID
#include "common/cextdecs.h"
#include <stdio.h>
#include <sys/time.h>
#include <sys/unistd.h>
#include <time.h>

#include "CatSQLShare.h"
#include "common/ComRegAPI.h"
#include "common/BaseTypes.h"
#include "common/ComASSERT.h"
#include "common/ComLocationNames.h"
#include "common/ComOperators.h"
#include "common/ComSmallDefs.h"

#ifdef _DEBUG
#include "common/ComRtUtils.h"
#endif

// This function now for non-NSKLite platforms only (UNIX)
long ComSmallDef_local_GetTimeStamp(void) { return (JULIANTIMESTAMP()); }

//**********************************************************************************
//
//  UID and funny name stuff
//
//

void ComUID::make_UID(void) {
// A UID is based on a 64-bit unique value
//
// For packaging purposes, the generation happens in CatSQLShare, see sqlshare/CatSQLShare.cpp
#ifdef _DEBUG
  // Debug code, to read the UID values from a file specified by an envvar
  char lineFromFile[80];

  if (ComRtGetValueFromFile("MX_FAKE_UID_FILE", lineFromFile, sizeof(lineFromFile))) {
    this->data = atoInt64(lineFromFile);
    return;
  }
#endif
  this->data = generateUniqueValue();
}

// This method was adapted from the definition of function
// void convertInt64ToAscii(const long &src, char* tgt)
// defined in w:/common/long.cpp
void ComUID::convertTo19BytesFixedWidthStringWithZeroesPrefix(ComString &tgt) const {
  long temp = get_value();
  char buffer[20];
  char *s = &buffer[occurs(buffer) - 1];
  memset(buffer, '0', occurs(buffer) - 1);
  *s = '\0';
  do {
    unsigned char c = (unsigned char)(temp % 10);
    *--s = (char)(c + '0');
    temp /= 10;
  } while (temp != 0);
  tgt = buffer;
}

// friend
ostream &operator<<(ostream &s, const ComUID &uid) {
  char buf[21];
  long num;
  int i;
  int digit;

  // "buf" is big enough to hold the biggest num possible.   We fill it
  // from right to left 'cause its easier then print from the first
  // (leftmost) valid character of the array
  buf[20] = 0;
  i = 19;
  num = uid.data;
  while (num > 0) {
    digit = (int)(num % 10);
    num = num / 10;
    buf[i--] = digit + '0';
  }
  i++;
  while (buf[i] != 0) {
    s << buf[i];
    i++;
  }

  return s;
}

// Return the 2-character string literal corresponding to the ComObjectType
// argument.
const char *comObjectTypeLit(ComObjectType objType) {
  switch (objType) {
    case COM_UNKNOWN_OBJECT:
      return COM_UNKNOWN_OBJECT_LIT;
    case COM_BASE_TABLE_OBJECT:
      return COM_BASE_TABLE_OBJECT_LIT;
    case COM_CHECK_CONSTRAINT_OBJECT:
      return COM_CHECK_CONSTRAINT_OBJECT_LIT;
    case COM_INDEX_OBJECT:
      return COM_INDEX_OBJECT_LIT;
    case COM_LIBRARY_OBJECT:
      return COM_LIBRARY_OBJECT_LIT;
    case COM_LOCK_OBJECT:
      return COM_LOCK_OBJECT_LIT;
    case COM_MODULE_OBJECT:
      return COM_MODULE_OBJECT_LIT;
    case COM_NOT_NULL_CONSTRAINT_OBJECT:
      return COM_NOT_NULL_CONSTRAINT_OBJECT_LIT;
    case COM_PRIMARY_KEY_CONSTRAINT_OBJECT:
      return COM_PRIMARY_KEY_CONSTRAINT_OBJECT_LIT;
    case COM_REFERENTIAL_CONSTRAINT_OBJECT:
      return COM_REFERENTIAL_CONSTRAINT_OBJECT_LIT;
    case COM_STORED_PROCEDURE_OBJECT:
      return COM_STORED_PROCEDURE_OBJECT_LIT;
    case COM_UNIQUE_CONSTRAINT_OBJECT:
      return COM_UNIQUE_CONSTRAINT_OBJECT_LIT;
    case COM_USER_DEFINED_ROUTINE_OBJECT:
      return COM_USER_DEFINED_ROUTINE_OBJECT_LIT;
    case COM_VIEW_OBJECT:
      return COM_VIEW_OBJECT_LIT;
    case COM_MV_OBJECT:
      return COM_MV_OBJECT_LIT;
    case COM_MVRG_OBJECT:
      return COM_MVRG_OBJECT_LIT;
    case COM_TRIGGER_OBJECT:
      return COM_TRIGGER_OBJECT_LIT;
    case COM_LOB_TABLE_OBJECT:
      return COM_LOB_TABLE_OBJECT_LIT;
    case COM_TRIGGER_TABLE_OBJECT:
      return COM_TRIGGER_TABLE_OBJECT_LIT;
    case COM_SYNONYM_OBJECT:
      return COM_SYNONYM_OBJECT_LIT;
    case COM_SHARED_SCHEMA_OBJECT:
      return COM_SHARED_SCHEMA_OBJECT_LIT;
    case COM_EXCEPTION_TABLE_OBJECT:
      return COM_EXCEPTION_TABLE_OBJECT_LIT;
    case COM_PRIVATE_SCHEMA_OBJECT:
      return COM_PRIVATE_SCHEMA_OBJECT_LIT;
    case COM_SEQUENCE_GENERATOR_OBJECT:
      return COM_SEQUENCE_GENERATOR_OBJECT_LIT;
    case COM_PACKAGE_OBJECT:
      return COM_PACKAGE_OBJECT_LIT;
  }

  ComASSERT(FALSE);
  return COM_UNKNOWN_OBJECT_LIT;
}

const char *comObjectTypeName(ComObjectType objType) {
  switch (objType) {
    case COM_UNKNOWN_OBJECT:
      return "UNKNOWN ";
    case COM_BASE_TABLE_OBJECT:
      return "TABLE ";
    case COM_INDEX_OBJECT:
      return "INDEX ";
    case COM_LIBRARY_OBJECT:
      return "LIBRARY ";
    case COM_STORED_PROCEDURE_OBJECT:
      return "PROCEDURE ";
    case COM_USER_DEFINED_ROUTINE_OBJECT:
      return "FUNCTION ";
    case COM_VIEW_OBJECT:
      return "VIEW ";
    // use a generic SCHEMA
    case COM_PRIVATE_SCHEMA_OBJECT:
    case COM_SHARED_SCHEMA_OBJECT:
      return "SCHEMA ";
    case COM_SEQUENCE_GENERATOR_OBJECT:
      return "SEQUENCE ";
    case COM_PACKAGE_OBJECT:
      return "PACKAGE ";
  }

  return "UNKNOWN ";
}
