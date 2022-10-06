/* -*-C++-*- */

/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ComRoutineActionNamePart.cpp
 * Description:  methods for class ComRoutineActionNamePart
 *
 *
 * Created:      11/07/09
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "ComRoutineActionNamePart.h"

#include "common/ComASSERT.h"
#include "common/nawstring.h"

// -----------------------------------------------------------------------
// friend functions
// -----------------------------------------------------------------------

//
// ostream::operator<<
//
ostream &operator<<(ostream &out, const ComRoutineActionNamePart &name) {
  out << "CLASS:  ComRoutineActionNamePart" << endl;

  ComString prettyUudfUid;
  name.getUudfUID().convertTo19BytesFixedWidthStringWithZeroesPrefix(prettyUudfUid /*out*/);

  out << "   UUDF UID        = 0" << prettyUudfUid.data() << endl;
  out << "   externalName_[] = ";
  out << '[' << name.getExternalName().length() << "] ";
  out << '"' << name.getExternalName().data() << '"' << endl;
  return (out);
}

// -----------------------------------------------------------------------
// Constructors
// -----------------------------------------------------------------------

ComRoutineActionNamePart::ComRoutineActionNamePart(CollHeap *h)
    : ComAnsiNamePart(h, (NASTRING_REGULAR_IDENT_WITH_DOLLAR_PREFIX | NASTRING_DELIM_IDENT_WITH_DOLLAR_PREFIX)),
      uudfUid_(0) {}

ComRoutineActionNamePart::ComRoutineActionNamePart(const ComRoutineActionNamePart &src, CollHeap *h)
    : ComAnsiNamePart(src, h), uudfUid_(src.getUudfUID()) {}

ComRoutineActionNamePart::ComRoutineActionNamePart(const ComUID &uudfUid, const char *namePart, size_t nameLen,
                                                   formatEnum format, CollHeap *h)
    : ComAnsiNamePart(namePart, nameLen, format, h,
                      (NASTRING_REGULAR_IDENT_WITH_DOLLAR_PREFIX | NASTRING_DELIM_IDENT_WITH_DOLLAR_PREFIX)),
      uudfUid_(uudfUid) {}

ComRoutineActionNamePart::ComRoutineActionNamePart(const ComUID &uudfUid, const NAString &namePart, formatEnum format,
                                                   CollHeap *h)
    : ComAnsiNamePart(namePart, format, h,
                      (NASTRING_REGULAR_IDENT_WITH_DOLLAR_PREFIX | NASTRING_DELIM_IDENT_WITH_DOLLAR_PREFIX)),
      uudfUid_(uudfUid) {}

ComRoutineActionNamePart::ComRoutineActionNamePart(const ComUID &uudfUid, const char *externalNameParts,
                                                   size_t externalNamePartsLen, size_t &count, CollHeap *h)
    : ComAnsiNamePart(externalNameParts, externalNamePartsLen, count, h,
                      (NASTRING_REGULAR_IDENT_WITH_DOLLAR_PREFIX | NASTRING_DELIM_IDENT_WITH_DOLLAR_PREFIX)),
      uudfUid_(uudfUid) {}

// -----------------------------------------------------------------------
// Virtual Destructor
// -----------------------------------------------------------------------

ComRoutineActionNamePart::~ComRoutineActionNamePart() {}

// -----------------------------------------------------------------------
// Virtual cast functions
// -----------------------------------------------------------------------

const ComRoutineActionNamePart *ComRoutineActionNamePart::castToComRoutineActionNamePart() const { return this; }

ComRoutineActionNamePart *ComRoutineActionNamePart::castToComRoutineActionNamePart() { return this; }

// -----------------------------------------------------------------------
// Operators
// -----------------------------------------------------------------------

//
// assignment operator
//
ComRoutineActionNamePart &ComRoutineActionNamePart::operator=(const ComRoutineActionNamePart &name) {
  setExternalName(name.getExternalName());
  setInternalName(name.getInternalName());
  toInternalIdentifierFlags_ = name.toInternalIdentifierFlags_;
  setUudfUID(name.getUudfUID());
  return *this;
}

//
// operator ==
//
NABoolean ComRoutineActionNamePart::operator==(const ComRoutineActionNamePart &rhs) const {
  return (this EQU & rhs OR(getInternalName() EQU rhs.getInternalName() AND getUudfUID() EQU rhs.getUudfUID()));
}

// -----------------------------------------------------------------------
// Accessors
// -----------------------------------------------------------------------

// The following method returns TRUE if parameter "output" is populated.
NABoolean ComRoutineActionNamePart::getRoutineActionNameStoredInOBJECTS_OBJECT_NAME(
    ComAnsiNamePart &output,  // out
    ComBoolean performCheck)  // in - default is TRUE
{
  if (NOT isValid()) return FALSE;

  ComString funnyNameInInternalFormat;
  getUudfUID().convertTo19BytesFixedWidthStringWithZeroesPrefix(funnyNameInInternalFormat /*out*/);
  funnyNameInInternalFormat += "_";
  funnyNameInInternalFormat += getInternalName();

  NAWString ucs2FunnyInternalName;
  ComAnsiNameToUCS2(funnyNameInInternalFormat, ucs2FunnyInternalName /* out */);
  if (performCheck AND ucs2FunnyInternalName.length() > MAX_IDENTIFIER_INT_LEN) return FALSE;

  NAString extName = ToAnsiIdentifier(funnyNameInInternalFormat, performCheck);
  if (extName.isNull())  // the generated external name is invalid
    return FALSE;

  output.setInternalName(funnyNameInInternalFormat);
  output.setExternalName(extName);
  return TRUE;
}

// -----------------------------------------------------------------------
// Mutators
// -----------------------------------------------------------------------

void ComRoutineActionNamePart::set(ComUID uudfUid, const NAString &externalFormatRoutineActionName) {
  setUudfUID(uudfUid);
  ((ComAnsiNamePart &)(*this)).operator=(externalFormatRoutineActionName);
}
