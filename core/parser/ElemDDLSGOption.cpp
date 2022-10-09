/* -*-C++-*-

 *****************************************************************************
 *
 * File:         ElemDDLSGOptions.C
 * Description:  methods for class ElemDDLSGOption and any classes
 *               derived from class ElemDDLSGOption.
 *
 * Created:      4/22/08
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "parser/ElemDDLSGOption.h"

#include "export/ComDiags.h"
#include "parser/ElemDDLSGOptions.h"
#define SQLPARSERGLOBALS_CONTEXT_AND_DIAGS
#include "common/NAString.h"
#include "parser/SqlParserGlobals.h"

// -----------------------------------------------------------------------
// methods for class ElemDDLSGOption
// -----------------------------------------------------------------------

// Default constructor
ElemDDLSGOption::ElemDDLSGOption(OperatorTypeEnum operType = ELM_SG_OPT_DEFAULT_ELEM) : ElemDDLNode(operType) {}

// virtual destructor
ElemDDLSGOption::~ElemDDLSGOption() {}

// casting
ElemDDLSGOption *ElemDDLSGOption::castToElemDDLSGOption() { return this; }

//
// methods for tracing
//

const NAString ElemDDLSGOption::getText() const {
  ABORT("internal logic error");
  return "ElemDDLSGOption";
}

// -----------------------------------------------------------------------
// methods for class ElemDDLSGOptionStartValue
// -----------------------------------------------------------------------

// constructor
ElemDDLSGOptionStartValue::ElemDDLSGOptionStartValue(long value)
    : ElemDDLSGOption(ELM_SG_OPT_START_VALUE_ELEM),
      value_(value) {}  // ElemDDLSGOptionStartValue::ElemDDLSGOptionStartValue()

// virtual destructor
ElemDDLSGOptionStartValue::~ElemDDLSGOptionStartValue() {}

// casting
ElemDDLSGOptionStartValue *ElemDDLSGOptionStartValue::castToElemDDLSGOptionStartValue() { return this; }

//
// methods for tracing
//

const NAString ElemDDLSGOptionStartValue::displayLabel1() const {
  return (NAString("Value: ") + Int64ToNAString((long)getValue()));
}

const NAString ElemDDLSGOptionStartValue::getText() const { return "ElemDDLSGOptionStartValue"; }

// method for building text
// virtual
NAString ElemDDLSGOptionStartValue::getSyntax() const {
  NAString syntax = "StartValue ";

  syntax += Int64ToNAString(value_);

  return syntax;
}  // getSyntax

// -----------------------------------------------------------------------
// methods for class ElemDDLSGOptionRestartValue
// -----------------------------------------------------------------------

// constructor
ElemDDLSGOptionRestartValue::ElemDDLSGOptionRestartValue(long value)
    : ElemDDLSGOption(ELM_SG_OPT_RESTART_VALUE_ELEM),
      value_(value) {}  // ElemDDLSGOptionRestartValue::ElemDDLSGOptionRestartValue()

// virtual destructor
ElemDDLSGOptionRestartValue::~ElemDDLSGOptionRestartValue() {}

// casting
ElemDDLSGOptionRestartValue *ElemDDLSGOptionRestartValue::castToElemDDLSGOptionRestartValue() { return this; }

//
// methods for tracing
//

const NAString ElemDDLSGOptionRestartValue::displayLabel1() const {
  return (NAString("Value: ") + Int64ToNAString((long)getValue()));
}

const NAString ElemDDLSGOptionRestartValue::getText() const { return "ElemDDLSGOptionRestartValue"; }

// method for building text
// virtual
NAString ElemDDLSGOptionRestartValue::getSyntax() const {
  NAString syntax = "RestartValue ";

  syntax += Int64ToNAString(value_);

  return syntax;
}  // getSyntax

// -----------------------------------------------------------------------
// methods for class ElemDDLSGOptionIncrement
// -----------------------------------------------------------------------

// constructor
ElemDDLSGOptionIncrement::ElemDDLSGOptionIncrement(long value)
    : ElemDDLSGOption(ELM_SG_OPT_INCREMENT_ELEM),
      value_(value) {}  // ElemDDLSGOptionIncrement::ElemDDLSGOptionIncrement()

// virtual destructor
ElemDDLSGOptionIncrement::~ElemDDLSGOptionIncrement() {}

// casting
ElemDDLSGOptionIncrement *ElemDDLSGOptionIncrement::castToElemDDLSGOptionIncrement() { return this; }

//
// methods for tracing
//

const NAString ElemDDLSGOptionIncrement::displayLabel1() const {
  return (NAString("Value: ") + Int64ToNAString((long)getValue()));
}

const NAString ElemDDLSGOptionIncrement::getText() const { return "ElemDDLSGOptionIncrement"; }

// method for building text
// virtual
NAString ElemDDLSGOptionIncrement::getSyntax() const {
  NAString syntax = "Increment ";

  syntax += Int64ToNAString(value_);
  return syntax;
}  // getSyntax

// -----------------------------------------------------------------------
// methods for class ElemDDLSGOptionMinValue
// -----------------------------------------------------------------------

// constructor
ElemDDLSGOptionMinValue::ElemDDLSGOptionMinValue(long value)
    : ElemDDLSGOption(ELM_SG_OPT_MIN_VALUE_ELEM),
      value_(value),
      isNoMinValue_(FALSE) {}  // ElemDDLSGOptionMinValue::ElemDDLSGOptionMinValue()

ElemDDLSGOptionMinValue::ElemDDLSGOptionMinValue(NABoolean noMinValue)
    : ElemDDLSGOption(ELM_SG_OPT_MIN_VALUE_ELEM),
      isNoMinValue_(noMinValue),
      value_(0) {}  // ElemDDLSGOptionMinValue::ElemDDLSGOptionMinValue()

// virtual destructor
ElemDDLSGOptionMinValue::~ElemDDLSGOptionMinValue() {}

// casting
ElemDDLSGOptionMinValue *ElemDDLSGOptionMinValue::castToElemDDLSGOptionMinValue() { return this; }

//
// methods for tracing
//

const NAString ElemDDLSGOptionMinValue::displayLabel1() const {
  return (NAString("Value: ") + Int64ToNAString((long)getValue()));
}

const NAString ElemDDLSGOptionMinValue::getText() const { return "ElemDDLSGOptionMinValue"; }

// method for building text
// virtual
NAString ElemDDLSGOptionMinValue::getSyntax() const {
  NAString syntax = "MinValue ";

  syntax += Int64ToNAString(value_);

  return syntax;
}  // getSyntax

// -----------------------------------------------------------------------
// methods for class ElemDDLSGOptionMaxValue
// -----------------------------------------------------------------------

// constructor
ElemDDLSGOptionMaxValue::ElemDDLSGOptionMaxValue(long value)
    : ElemDDLSGOption(ELM_SG_OPT_MAX_VALUE_ELEM),
      value_(value),
      isNoMaxValue_(FALSE) {}  // ElemDDLSGOptionMaxValue::ElemDDLSGOptionMaxValue()

ElemDDLSGOptionMaxValue::ElemDDLSGOptionMaxValue(NABoolean noMaxValue)
    : ElemDDLSGOption(ELM_SG_OPT_MAX_VALUE_ELEM),
      isNoMaxValue_(noMaxValue),
      value_(0) {}  // ElemDDLSGOptionMinValue::ElemDDLSGOptionMinValue()

// virtual destructor
ElemDDLSGOptionMaxValue::~ElemDDLSGOptionMaxValue() {}

// casting
ElemDDLSGOptionMaxValue *ElemDDLSGOptionMaxValue::castToElemDDLSGOptionMaxValue() { return this; }

//
// methods for tracing
//

const NAString ElemDDLSGOptionMaxValue::displayLabel1() const {
  return (NAString("Value: ") + Int64ToNAString((long)getValue()));
}

const NAString ElemDDLSGOptionMaxValue::getText() const { return "ElemDDLSGOptionMaxValue"; }

// method for building text
// virtual
NAString ElemDDLSGOptionMaxValue::getSyntax() const {
  NAString syntax = "MaxValue ";

  syntax += Int64ToNAString(value_);

  return syntax;
}  // getSyntax

// -----------------------------------------------------------------------
// methods for class ElemDDLSGOptionCycleOption
// -----------------------------------------------------------------------

// constructor

ElemDDLSGOptionCycleOption::ElemDDLSGOptionCycleOption(NABoolean cycleOption)
    : ElemDDLSGOption(ELM_SG_OPT_CYCLE_OPTION_ELEM),
      cycle_(cycleOption) {}  // ElemDDLSGOptionCycleOption::ElemDDLSGOptionCycleOption()

ElemDDLSGOptionCycleOption::ElemDDLSGOptionCycleOption()
    : ElemDDLSGOption(ELM_SG_OPT_CYCLE_OPTION_ELEM),
      cycle_(FALSE) {}  // ElemDDLSGOptionCycleOption::ElemDDLSGOptionCycleOption()

// virtual destructor
ElemDDLSGOptionCycleOption::~ElemDDLSGOptionCycleOption() {}

// casting
ElemDDLSGOptionCycleOption *ElemDDLSGOptionCycleOption::castToElemDDLSGOptionCycleOption() { return this; }

//
// methods for tracing
//

const NAString ElemDDLSGOptionCycleOption::displayLabel1() const {
  if (cycle_ == TRUE)
    return (NAString("CYCLE "));
  else
    return (NAString("NO CYCLE "));
  ;
}

const NAString ElemDDLSGOptionCycleOption::getText() const { return "ElemDDLSGOptionCycleOption"; }

// method for building text
// virtual
NAString ElemDDLSGOptionCycleOption::getSyntax() const {
  NAString syntax = "CycleOption: ";
  if (cycle_ == TRUE)
    syntax += "CYCLE";
  else
    syntax += "NO CYCLE";

  return syntax;
}  // getSyntax

// -----------------------------------------------------------------------
// methods for class ElemDDLSGOptionOrderOption
// -----------------------------------------------------------------------

ElemDDLSGOptionOrderOption::ElemDDLSGOptionOrderOption(NABoolean isNoOrder)
    : ElemDDLSGOption(ELM_SG_OPT_ORDER_OPTION_ELEM) {
  isNoOrder_ = isNoOrder;
}  // ElemDDLSGOptionOrderOption::ElemDDLSGOptionOrderOption()

// virtual destructor
ElemDDLSGOptionOrderOption::~ElemDDLSGOptionOrderOption() {}

// casting
ElemDDLSGOptionOrderOption *ElemDDLSGOptionOrderOption::castToElemDDLSGOptionOrderOption() { return this; }

// -----------------------------------------------------------------------
// methods for class ElemDDLSGOptionCacheOption
// -----------------------------------------------------------------------

// constructor

ElemDDLSGOptionCacheOption::ElemDDLSGOptionCacheOption(NABoolean cache, long cacheSize)
    : ElemDDLSGOption(ELM_SG_OPT_CACHE_OPTION_ELEM),
      cache_(cache),
      cacheSize_(cacheSize) {}  // ElemDDLSGOptionCacheOption::ElemDDLSGOptionCacheOption()

ElemDDLSGOptionCacheOption::ElemDDLSGOptionCacheOption()
    : ElemDDLSGOption(ELM_SG_OPT_CACHE_OPTION_ELEM),
      cache_(FALSE),
      cacheSize_(0) {}  // ElemDDLSGOptionCacheOption::ElemDDLSGOptionCacheOption()

// virtual destructor
ElemDDLSGOptionCacheOption::~ElemDDLSGOptionCacheOption() {}

// casting
ElemDDLSGOptionCacheOption *ElemDDLSGOptionCacheOption::castToElemDDLSGOptionCacheOption() { return this; }

//
// methods for tracing
//

const NAString ElemDDLSGOptionCacheOption::displayLabel1() const {
  if (cache_)
    return (NAString("CACHE "));
  else
    return (NAString("NO CACHE "));
  ;
}

const NAString ElemDDLSGOptionCacheOption::getText() const { return "ElemDDLSGOptionCacheOption"; }

// method for building text
// virtual
NAString ElemDDLSGOptionCacheOption::getSyntax() const {
  NAString syntax = "CacheOption: ";
  if (cache_ > 0)
    syntax += "CACHE";
  else
    syntax += "NO CACHE";

  return syntax;
}  // getSyntax
NAString ElemDDLSGOptionSystemOption::getSyntax() const {
  NAString syntax = "GLOBAL";
  return syntax;
};
// -----------------------------------------------------------------------
// methods for class ElemDDLSGOptionDatatype
// -----------------------------------------------------------------------

// constructor

ElemDDLSGOptionDatatype::ElemDDLSGOptionDatatype(ComFSDataType dt)
    : ElemDDLSGOption(ELM_SG_OPT_DATATYPE_ELEM), dt_(dt) {}  // ElemDDLSGOptionDatatype::ElemDDLSGOptionDatatype()

ElemDDLSGOptionDatatype::ElemDDLSGOptionDatatype()
    : ElemDDLSGOption(ELM_SG_OPT_DATATYPE_ELEM),
      dt_(COM_UNKNOWN_FSDT) {}  // ElemDDLSGOptionDatatype::ElemDDLSGOptionDatatype()

// virtual destructor
ElemDDLSGOptionDatatype::~ElemDDLSGOptionDatatype() {}

// casting
ElemDDLSGOptionDatatype *ElemDDLSGOptionDatatype::castToElemDDLSGOptionDatatype() { return this; }

//
// methods for tracing
//

const NAString ElemDDLSGOptionDatatype::displayLabel1() const { return (NAString("DATATYPE ")); }

const NAString ElemDDLSGOptionDatatype::getText() const { return "ElemDDLSGOptionDatatype"; }

// method for building text
// virtual
NAString ElemDDLSGOptionDatatype::getSyntax() const {
  NAString syntax = "Datatype: ";

  return syntax;
}  // getSyntax

// -----------------------------------------------------------------------
// methods for class ElemDDLSGOptionResetOption
// -----------------------------------------------------------------------

// constructor

ElemDDLSGOptionResetOption::ElemDDLSGOptionResetOption()
    : ElemDDLSGOption(ELM_SG_OPT_RESET_OPTION_ELEM) {}  // ElemDDLSGOptionResetOption::ElemDDLSGOptionResetOption()

// virtual destructor
ElemDDLSGOptionResetOption::~ElemDDLSGOptionResetOption() {}

// casting
ElemDDLSGOptionResetOption *ElemDDLSGOptionResetOption::castToElemDDLSGOptionResetOption() { return this; }

//
// methods for tracing
//

const NAString ElemDDLSGOptionResetOption::displayLabel1() const { return (NAString("RESET ")); }

const NAString ElemDDLSGOptionResetOption::getText() const { return "ElemDDLSGOptionResetOption"; }

// method for building text
// virtual
NAString ElemDDLSGOptionResetOption::getSyntax() const {
  NAString syntax = "RESET";

  return syntax;
}  // getSyntax

// -----------------------------------------------------------------------
// methods for class ElemDDLSGOptionNextValOption
// -----------------------------------------------------------------------

// constructor

ElemDDLSGOptionNextValOption::ElemDDLSGOptionNextValOption(long nextVal)
    : ElemDDLSGOption(ELM_SG_OPT_NEXTVAL_OPTION_ELEM),
      nextVal_(nextVal) {}  // ElemDDLSGOptionNextValOption::ElemDDLSGOptionNextValOption

// virtual destructor
ElemDDLSGOptionNextValOption::~ElemDDLSGOptionNextValOption() {}

// casting
ElemDDLSGOptionNextValOption *ElemDDLSGOptionNextValOption::castToElemDDLSGOptionNextValOption() { return this; }

//
// methods for tracing
//

const NAString ElemDDLSGOptionNextValOption::displayLabel1() const {
  return (NAString("NEXTVAL: ") + Int64ToNAString((long)getNextVal()));
}

const NAString ElemDDLSGOptionNextValOption::getText() const { return "ElemDDLSGOptionNextVal"; }

// method for building text
// virtual
NAString ElemDDLSGOptionNextValOption::getSyntax() const {
  NAString syntax = "NextVal ";
  syntax += Int64ToNAString(nextVal_);
  return syntax;
}  // getSyntax

//
// End of File
//
