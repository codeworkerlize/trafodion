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

#include "parser/ElemDDLLoadOptions.h"

#include "export/ComDiags.h"
#define SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#include "common/NAString.h"
#include "parser/SqlParserGlobals.h"

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

//
// End of File
//
