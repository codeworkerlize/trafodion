/* -*-C++-*-

 *****************************************************************************
 *
 * File:         ElemDDLLoadOptions.C
 * Description:  methods for class ElemDDLLoadOpt and any classes
 *               derived from class ElemDDLLoadOpt.
 *
 * Created:      9/28/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "export/ComDiags.h"
#include "ElemDDLLoadOptions.h"
#define SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#include "parser/SqlParserGlobals.h"
#include "common/NAString.h"

// -----------------------------------------------------------------------
// methods for class ElemDDLLoadOpt
// -----------------------------------------------------------------------

// virtual destructor
ElemDDLLoadOpt::~ElemDDLLoadOpt() {}

// casting
ElemDDLLoadOpt *ElemDDLLoadOpt::castToElemDDLLoadOpt() { return this; }

//
// methods for tracing
//

const NAString ElemDDLLoadOpt::getText() const {
  ABORT("internal logic error");
  return "ElemDDLLoadOpt";
}

// -----------------------------------------------------------------------
// methods for class ElemDDLLoadOptDSlack
// -----------------------------------------------------------------------

// constructor
ElemDDLLoadOptDSlack::ElemDDLLoadOptDSlack(int percentage) : ElemDDLLoadOpt(ELM_LOAD_OPT_D_SLACK_ELEM) {
  if (isLegalPercentageValue(percentage)) {
    percentage_ = (unsigned short)percentage;
  } else {
    percentage_ = DEFAULT_PERCENTAGE;
    // illegal percentage value in DSLACK phrase.
    *SqlParser_Diags << DgSqlCode(-3060);
  }
}  // ElemDDLLoadOptDSlack::ElemDDLLoadOptDSlack()

// virtual destructor
ElemDDLLoadOptDSlack::~ElemDDLLoadOptDSlack() {}

// casting
ElemDDLLoadOptDSlack *ElemDDLLoadOptDSlack::castToElemDDLLoadOptDSlack() { return this; }

//
// accessors
//

// is the specified percentage a legal value?
NABoolean ElemDDLLoadOptDSlack::isLegalPercentageValue(int percentage) {
  if (percentage >= 0 AND percentage <= 100) {
    return TRUE;
  } else {
    return FALSE;
  }
}

//
// methods for tracing
//

const NAString ElemDDLLoadOptDSlack::displayLabel1() const {
  return (NAString("Percentage: ") + LongToNAString((int)getPercentage()));
}

const NAString ElemDDLLoadOptDSlack::getText() const { return "ElemDDLLoadOptDSlack"; }

// method for building text
// virtual
NAString ElemDDLLoadOptDSlack::getSyntax() const {
  NAString syntax = "DSLACK ";

  syntax += UnsignedToNAString(percentage_);

  return syntax;
}  // getSyntax

// -----------------------------------------------------------------------
// methods for class ElemDDLLoadOptISlack
// -----------------------------------------------------------------------

// constructor
ElemDDLLoadOptISlack::ElemDDLLoadOptISlack(int percentage) : ElemDDLLoadOpt(ELM_LOAD_OPT_I_SLACK_ELEM) {
  if (isLegalPercentageValue(percentage)) {
    percentage_ = (unsigned short)percentage;
  } else {
    percentage_ = DEFAULT_PERCENTAGE;
    // illegal percentage value in ISLACK phrase.
    *SqlParser_Diags << DgSqlCode(-3060);
  }
}  // ElemDDLLoadOptISlack::ElemDDLLoadOptISlack()

// virtual destructor
ElemDDLLoadOptISlack::~ElemDDLLoadOptISlack() {}

// casting
ElemDDLLoadOptISlack *ElemDDLLoadOptISlack::castToElemDDLLoadOptISlack() { return this; }

//
// accessors
//

// is the specified percentage a legal value?
NABoolean ElemDDLLoadOptISlack::isLegalPercentageValue(int percentage) {
  if (percentage >= 0 AND percentage <= 100) {
    return TRUE;
  } else {
    return FALSE;
  }
}

//
// methods for tracing
//

const NAString ElemDDLLoadOptISlack::displayLabel1() const {
  return (NAString("Percentage: ") + LongToNAString((int)getPercentage()));
}

const NAString ElemDDLLoadOptISlack::getText() const { return "ElemDDLLoadOptISlack"; }

// method for building text
// virtual
NAString ElemDDLLoadOptISlack::getSyntax() const {
  NAString syntax = "ISLACK ";

  syntax += UnsignedToNAString(percentage_);
  return syntax;
}  // getSyntax

//
// End of File
//
