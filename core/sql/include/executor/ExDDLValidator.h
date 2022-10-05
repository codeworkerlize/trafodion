/*********************************************************************

**********************************************************************/
#ifndef __EX_DDLVALIDATOR_H__
#define __EX_DDLVALIDATOR_H__

/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ExDDLValidator.h
 * Description:  This object is used to insure that the DDL definition of
 *               an object is still valid at run time.
 *
 * Created:      10/11/2019
 * Language:     C++
 *
 *****************************************************************************
 */

#include "executor/ExStats.h"
#include "runtimestats/rts_msg.h"  // to get ObjectEpochChangeRequest flags

//////////////////////////////////////////////////////////////////
// classes defined in this file
//////////////////////////////////////////////////////////////////
class ExDDLValidator;

//////////////////////////////////////////////////////////////////////
// This class is used for a fast check of DDL validation at run-time.
// The objective is to insure, as best as possible and as efficiently
// as possible, that when we perform an I/O access to an object,
// the DDL definition of that object is still valid.
//
// As of now, the memory model is very simple. The users of this
// class are responsible for creating and destroying any object
// passed to this class. So, for example, this class never deletes
// anything that it points to.
//////////////////////////////////////////////////////////////////////
class ExDDLValidator {
 public:
  ExDDLValidator(UInt32 expectedEpoch, UInt32 expectedFlags)
      : expectedEpoch_(expectedEpoch), expectedFlags_(expectedFlags), oecEntry_(NULL){/* nothing else to do */};
  ~ExDDLValidator(){/* nothing to do */};
  void setValidationInfo(ObjectEpochCacheEntry *oecEntry) { oecEntry_ = oecEntry; }
  bool validatingDDL() { return (oecEntry_ != NULL); }

  // This method is used for checking write references
  bool isDDLValid() {
    if (oecEntry_) return (oecEntry_->epoch() == expectedEpoch_) && (oecEntry_->flags() == expectedFlags_);
    return true;
  }

  // This method is used for checking read references
  bool isDDLValidForReads() {
    if (oecEntry_) {
      if (oecEntry_->epoch() != expectedEpoch_) return false;
      if (oecEntry_->flags() == expectedFlags_) return true;
      if ((oecEntry_->flags() == ObjectEpochChangeRequest::DDL_IN_PROGRESS) &&
          (expectedFlags_ == ObjectEpochChangeRequest::NO_DDL_IN_PROGRESS))
        // the READS_DISALLOWED flag is not set, so allow it
        return true;
      else
        return false;
    }
    return true;
  }

 private:
  // compile time values
  const UInt32 expectedEpoch_;
  const UInt32 expectedFlags_;  // TODO: will be 0 except perhaps for DML in a mixed DDL/DML transaction

  // pointer to runtime values
  ObjectEpochCacheEntry *oecEntry_;  // NULL if no validation is to be done
};

#endif
