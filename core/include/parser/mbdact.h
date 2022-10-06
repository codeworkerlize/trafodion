
/* -*-C++-*-
******************************************************************************
*
* File:         mbdact.h
* Description:  common embedded exception action definitions shared
*               between the preprocessor and parser.
* Created:      7/8/95
* Language:     C++
*
*

*
*
******************************************************************************
*/

// -----------------------------------------------------------------------
// Change history:
//
//
// -----------------------------------------------------------------------

#ifndef MBDACT_H
#define MBDACT_H

enum MBD_CONDITION { onError, on_Warning, onWarning, onEnd, MAX_MBD_CONDITION };

enum MBD_ACTION { NULL_ACTION, CALL_ACTION, GOTO_ACTION, MAX_ACTION };

class action {
 public:
  action();
  NABoolean isNull() const;
  void setActionLabel(MBD_ACTION, const NAString &);

  MBD_ACTION theAction;
  NAString theLabel;  // can be goto label, or name of a function
};

// set theAction to NULL_ACTION
inline action::action() { theAction = NULL_ACTION; }

// return TRUE iff theAction == NULL_ACTION
inline NABoolean action::isNull(void) const { return (theAction == NULL_ACTION); }

#endif
