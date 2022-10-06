#ifndef DOMAINDESC_H
#define DOMAINDESC_H
/* -*-C++-*-
**************************************************************************
*
* File:         DomainDesc.h
* Description:  A Domain descriptor
* Created:      4/27/94
* Language:     C++
*
*

*
*
**************************************************************************
*/

// -----------------------------------------------------------------------

#include "common/BaseTypes.h"
#include "common/CmpCommon.h"  // CMPASSERT
#include "common/Collections.h"
#include "common/NAType.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class DomainDesc;
class DomainDescList;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
class SchemaDB;

// ***********************************************************************
// DomainDesc
//
// A descriptor for a Domain
// ***********************************************************************

class DomainDesc : public NABasicObject {
 public:
  // ---------------------------------------------------------------------
  // Constructor functions
  // ---------------------------------------------------------------------
  DomainDesc(SchemaDB *schemaDB, const NAType &refToType);

  // ---------------------------------------------------------------------
  // Accessor functions
  // ---------------------------------------------------------------------
  const NAType &getType() const { return *type_; }

  void changeType(const NAType *type) {
    CMPASSERT(type);
    type_ = type;
  }

  NAString getTypeSQLname(NABoolean terse = FALSE) const { return getType().getTypeSQLname(terse); }

  const NAString getDomainName() const { return getTypeSQLname(); }

  // ---------------------------------------------------------------------
  // A default value may be associated with a column.
  // ---------------------------------------------------------------------
  ////	inline void setDefaultValue(void *bufPtr, long buflen);
  ////	inline void getDefaultValue(void *bufPtr, long *bufLen) const;

  // ---------------------------------------------------------------------
  // The following methods are required for each descriptor.
  // ---------------------------------------------------------------------
  NABoolean operator==(const DomainDesc &other) const { return (getTypeSQLname() == other.getTypeSQLname()); }

 private:
  // ---------------------------------------------------------------------
  // The actual descriptor for the type.
  // ---------------------------------------------------------------------
  const NAType *type_;

  // ---------------------------------------------------------------------
  // The default value is stored in a buffer that is of the nominal
  // size for the data type to which this column belongs (maximum size
  // for character strings.
  // The storage is untyped.
  // ---------------------------------------------------------------------
  ////	void  *defaultvalue_;

};  // class DomainDesc

// ***********************************************************************
// Implementation for inline functions
// ***********************************************************************

// -----------------------------------------------------------------------
// Accessor function for the default value
// -----------------------------------------------------------------------

////	void DomainDesc::getDefaultValue(void* bufPtr, long *bufLen) const
////	{
////	  char *dstp = (char *)bufPtr;
////	  char *srcp = (char *)defaultvalue_; // Use the size of the data type below
////	  for (int index = 0; index < *bufLen; index++)
////	    *dstp[index] = *srcp[index];
////	}

////	void DomainDesc::setDefaultValue(void *bufPtr, long buflen)
////	{
////	  char *srcp = (char *)bufPtr;
////	  char *dstp = (char *)defaultvalue_;
////	  for (int index = 0; index < buflen; index++)
////	    *dstp[index] = *srcp[index];
////	}

// ***********************************************************************
// DomainDescList
//
// A collection of descriptors for a Domain
// ***********************************************************************

class DomainDescList : public LIST(DomainDesc *) {
 public:
  DomainDescList(CollHeap *h /*=0*/) : LIST(DomainDesc *)(h) {}
};  // class DomainDescList

#endif /* DOMAINDESC_H */
