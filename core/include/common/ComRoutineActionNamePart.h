/* -*-C++-*- */

#ifndef COMROUTINEACTIONNAMEPART_H
#define COMROUTINEACTIONNAMEPART_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ComRoutineActionNamePart.h
 * Description:  An object of this class represents the routine action
 *               name part in a fully qualified routine action name.
 *
 * Created:      11/07/09
 * Language:     C++
 *
 *
 *****************************************************************************
 */

#include <iosfwd>

#include "common/NAWinNT.h"  // for wchar (tcr)
#include "common/Platform.h"
using namespace std;
#include <string.h>

#include "common/ComAnsiNamePart.h"
#include "common/ComOperators.h"
#include "common/ComSizeDefs.h"
#include "common/ComSmallDefs.h"
#include "common/NABoolean.h"
#include "common/NAString.h"

// -----------------------------------------------------------------------
// forward declarations
// -----------------------------------------------------------------------
class ComAnsiNamePart;
class ComRoutineActionNamePart;

// -----------------------------------------------------------------------
// definition of class ComRoutineActionNamePart
// -----------------------------------------------------------------------

class ComRoutineActionNamePart : public ComAnsiNamePart {
  //
  // global friend functions
  //

  friend ostream &operator<<(ostream &out, const ComAnsiNamePart &name);

 public:
  //
  // constructors
  //

  // Default constructor.  This method creates an empty object.
  ComRoutineActionNamePart(CollHeap *h = 0);

  // Copy constructors.
  // If the specified/input name is legal, construct the object
  // from the input name component; otherwise, construct an
  // empty object.

  ComRoutineActionNamePart(const ComRoutineActionNamePart &orig, CollHeap *h = 0);

  ComRoutineActionNamePart(const ComUID &uudfUid, const NAString &name, formatEnum format = EXTERNAL_FORMAT,
                           CollHeap *h = 0);

  ComRoutineActionNamePart(const ComUID &uudfUid, const char *name, size_t nameLenInBytes,
                           formatEnum format = EXTERNAL_FORMAT, CollHeap *h = 0);

  ComRoutineActionNamePart(const ComUID &uudfUid, const char *externalNameParts, size_t externalNPLen, size_t &count,
                           CollHeap *h = 0);

  //
  // virtual destructor
  //
  virtual ~ComRoutineActionNamePart();

  //
  // virtual cast function
  //
  virtual ComRoutineActionNamePart *castToComRoutineActionNamePart();
  virtual const ComRoutineActionNamePart *castToComRoutineActionNamePart() const;

  //
  // assignment operators
  //

  ComRoutineActionNamePart &operator=(const ComRoutineActionNamePart &);

  //
  // logical operator
  //
  NABoolean operator==(const ComRoutineActionNamePart &rhs) const;

  //
  // accessors
  //

  const ComUID &getUudfUID() const { return uudfUid_; }
  ComUID &getUudfUID() { return uudfUid_; }

  // The following method returns TRUE if parameter "output" is populated.
  // By default, checks this object and the generated output to make sure
  // that they are valid.  If the input parameter "performCheck" is set to
  // FALSE, avoid checking the validity of the generated output.
  NABoolean getRoutineActionNameStoredInOBJECTS_OBJECT_NAME(ComAnsiNamePart &output, ComBoolean performCheck = TRUE);

  //
  // mutators
  //

  void setUudfUID(const ComUID &uudfUid) { uudfUid_ = uudfUid; }

  void set(ComUID uudfUid, const NAString &externalFormatRoutineActionName);

  // The specified externalFormatRoutineActionName must be a
  // valid name; otherwise, this object will be cleared.

  void clear() {
    ComAnsiNamePart::clear();
    uudfUid_ = 0;
  }

  // Makes this object an empty object.

 private:
  ComUID uudfUid_;  // UUDF (e.g., SAS_PUT) UID

  // ---------------------------------------------------------------------
  // private methods
  // ---------------------------------------------------------------------

  // The following methods are not defined - DO NOT USE them
  ComRoutineActionNamePart &operator=(const NAString &externalName);

};  // class ComRoutineActionNamePart

#endif  // COMROUTINEACTIONNAMEPART_H
