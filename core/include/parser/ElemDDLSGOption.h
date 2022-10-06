
#ifndef ELEMDDLSGOPTION_H
#define ELEMDDLSGOPTION_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLSGOption.h
 * Description:  classes for sequence generator options specified in DDL statements
 *
 * Created:      4/22/08
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "ElemDDLNode.h"
#include "common/BaseTypes.h"
#include "parser/ElemDDLSGOptions.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLSGOption;
class ElemDDLSGOptionStartValue;
class ElemDDLSGOptionIncrement;
class ElemDDLSGOptionMinValue;
class ElemDDLSGOptionMaxValue;
class ElemDDLSGOptionCacheOption;
class ElemDDLSGOptionOrderOption;
class ElemDDLSGOptionCycleOption;
class ElemDDLSGOptionDatatype;
class ElemDDLSGOptionResetOption;
class ElemDDLSGOptionSystemOption;
class ElemDDLSGOptionNextValOption;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None

// -----------------------------------------------------------------------
// definition of base class ElemDDLSGOption
// -----------------------------------------------------------------------
class ElemDDLSGOption : public ElemDDLNode {
 public:
  // default constructor
  ElemDDLSGOption(OperatorTypeEnum operatorType);

  // virtual destructor
  virtual ~ElemDDLSGOption();

  // cast
  virtual ElemDDLSGOption *castToElemDDLSGOption();

  // method for tracing
  virtual const NAString getText() const;

};  // class ElemDDLSGOption

// -----------------------------------------------------------------------
// definition of class ElemDDLSGOptionStartValue
// -----------------------------------------------------------------------
class ElemDDLSGOptionStartValue : public ElemDDLSGOption {
 public:
  // constructors
  ElemDDLSGOptionStartValue(long value);

  // virtual destructor
  virtual ~ElemDDLSGOptionStartValue();

  // cast
  virtual ElemDDLSGOptionStartValue *castToElemDDLSGOptionStartValue();

  // accessors
  inline long getValue() const;

  // methods for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString getText() const;

  // method for building text
  virtual NAString getSyntax() const;

 private:
  //
  // data member(s)
  //

  long value_;

};  // class ElemDDLSGOptionStartValue

// -----------------------------------------------------------------------
// definition of class ElemDDLSGOptionRestartValue
// -----------------------------------------------------------------------
class ElemDDLSGOptionRestartValue : public ElemDDLSGOption {
 public:
  // constructors
  ElemDDLSGOptionRestartValue(long value);

  // virtual destructor
  virtual ~ElemDDLSGOptionRestartValue();

  // cast
  virtual ElemDDLSGOptionRestartValue *castToElemDDLSGOptionRestartValue();

  // accessors
  inline long getValue() const;

  // methods for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString getText() const;

  // method for building text
  virtual NAString getSyntax() const;

 private:
  //
  // data member(s)
  //

  long value_;

};  // class ElemDDLSGOptionRestartValue

// -----------------------------------------------------------------------
// definition of class ElemDDLSGOptionIncrement
// -----------------------------------------------------------------------
class ElemDDLSGOptionIncrement : public ElemDDLSGOption {
 public:
  // constructor
  ElemDDLSGOptionIncrement(long value);

  // virtual destructor
  virtual ~ElemDDLSGOptionIncrement();

  // cast
  virtual ElemDDLSGOptionIncrement *castToElemDDLSGOptionIncrement();

  // accessors
  inline long getValue() const;

  // methods for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString getText() const;

  // method for building text
  virtual NAString getSyntax() const;

 private:
  //
  // data member(s)
  //

  long value_;

};  // class ElemDDLSGOptionIncrement

// -----------------------------------------------------------------------
// definition of class ElemDDLSGOptionMinValue
// -----------------------------------------------------------------------
class ElemDDLSGOptionMinValue : public ElemDDLSGOption {
 public:
  // constructor
  ElemDDLSGOptionMinValue(long value);
  ElemDDLSGOptionMinValue(NABoolean noMinValue);

  // virtual destructor
  virtual ~ElemDDLSGOptionMinValue();

  // cast
  virtual ElemDDLSGOptionMinValue *castToElemDDLSGOptionMinValue();

  // accessors
  inline long getValue() const;
  inline NABoolean isNoMinValue() const;

  // methods for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString getText() const;

  // method for building text
  virtual NAString getSyntax() const;

 private:
  //
  // data member(s)
  //

  long value_;
  NABoolean isNoMinValue_;

};  // class ElemDDLSGOptionMinValue

// -----------------------------------------------------------------------
// definition of class ElemDDLSGOptionMaxValue
// -----------------------------------------------------------------------
class ElemDDLSGOptionMaxValue : public ElemDDLSGOption {
 public:
  // constructor
  ElemDDLSGOptionMaxValue(long value);
  ElemDDLSGOptionMaxValue(NABoolean noMaxValue);

  // virtual destructor
  virtual ~ElemDDLSGOptionMaxValue();

  // cast
  virtual ElemDDLSGOptionMaxValue *castToElemDDLSGOptionMaxValue();

  // accessors
  inline long getValue() const;
  inline NABoolean isNoMaxValue() const;

  // methods for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString getText() const;

  // method for building text
  virtual NAString getSyntax() const;

 private:
  //
  // data member(s)
  //

  long value_;
  NABoolean isNoMaxValue_;

};  // class ElemDDLSGOptionMaxValue

// -----------------------------------------------------------------------
// definition of class ElemDDLSGOptionCycleOption
// -----------------------------------------------------------------------
class ElemDDLSGOptionCycleOption : public ElemDDLSGOption {
 public:
  // constructor
  ElemDDLSGOptionCycleOption(NABoolean cycle);
  ElemDDLSGOptionCycleOption();

  // virtual destructor
  virtual ~ElemDDLSGOptionCycleOption();

  // cast
  virtual ElemDDLSGOptionCycleOption *castToElemDDLSGOptionCycleOption();

  // accessors
  inline NABoolean isCycle() const { return cycle_ == TRUE; };
  inline NABoolean isNoCycle() const { return cycle_ == FALSE; };
  inline NABoolean getValue() const { return cycle_; };

  // methods for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString getText() const;

  // method for building text
  virtual NAString getSyntax() const;

 private:
  //
  // data member(s)
  //

  NABoolean cycle_;

};  // class ElemDDLSGOptionCycleOption

// -----------------------------------------------------------------------
// definition of class ElemDDLSGOptionCacheOption
// -----------------------------------------------------------------------
class ElemDDLSGOptionCacheOption : public ElemDDLSGOption {
 public:
  // constructor
  ElemDDLSGOptionCacheOption(NABoolean cache, long cacheSize);
  ElemDDLSGOptionCacheOption();

  // virtual destructor
  virtual ~ElemDDLSGOptionCacheOption();

  // cast
  virtual ElemDDLSGOptionCacheOption *castToElemDDLSGOptionCacheOption();

  // accessors
  inline NABoolean isCache() const { return cache_ == TRUE; };
  inline NABoolean isNoCache() const { return cache_ == FALSE; };
  inline long getCacheSize() const { return cacheSize_; };

  // methods for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString getText() const;

  // method for building text
  virtual NAString getSyntax() const;

 private:
  //
  // data member(s)
  //
  NABoolean cache_;
  long cacheSize_;

};  // class ElemDDLSGOptionCacheOption

// -----------------------------------------------------------------------
// definition of class ElemDDLSGOptionOrderOption
// -----------------------------------------------------------------------
class ElemDDLSGOptionOrderOption : public ElemDDLSGOption {
 public:
  // constructor
  ElemDDLSGOptionOrderOption(NABoolean isNoOrder = false);

  // virtual destructor
  virtual ~ElemDDLSGOptionOrderOption();

  virtual ElemDDLSGOptionOrderOption *castToElemDDLSGOptionOrderOption();

  NABoolean isNoOrder() { return isNoOrder_; }

 private:
  NABoolean isNoOrder_;

};  // class ElemDDLSGOptionOrderOption

// -----------------------------------------------------------------------
// definition of class ElemDDLSGOptionSystemOption
// -----------------------------------------------------------------------
class ElemDDLSGOptionSystemOption : public ElemDDLSGOption {
 public:
  // constructor
  ElemDDLSGOptionSystemOption(long timeout = -1) : ElemDDLSGOption(ELM_SG_OPT_SYSTEM_OPTION_ELEM) {
    timeout_ = timeout;
  };

  // virtual destructor
  virtual ~ElemDDLSGOptionSystemOption(){};

  virtual ElemDDLSGOptionSystemOption *castToElemDDLSGOptionSystemOption() { return this; };

  // methods for tracing
  virtual const NAString displayLabel1() const { return (NAString("GLOBAL ")); };
  virtual const NAString getText() const { return "ElemDDLSGOptionSystemOption"; };

  // method for building text
  virtual NAString getSyntax() const;
  long getTimeout() { return timeout_; };

 private:
  long timeout_;  // in microseconds.

};  // class ElemDDLSGOptionOrderOption
// -----------------------------------------------------------------------
// definition of class ElemDDLSGOptionDatatype
// -----------------------------------------------------------------------
class ElemDDLSGOptionDatatype : public ElemDDLSGOption {
 public:
  // constructor
  ElemDDLSGOptionDatatype(ComFSDataType dt);
  ElemDDLSGOptionDatatype();

  // virtual destructor
  virtual ~ElemDDLSGOptionDatatype();

  // cast
  virtual ElemDDLSGOptionDatatype *castToElemDDLSGOptionDatatype();

  // accessors
  inline ComFSDataType getDatatype() { return dt_; }

  // methods for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString getText() const;

  // method for building text
  virtual NAString getSyntax() const;

 private:
  ComFSDataType dt_;

};  // class ElemDDLSGOptionDatatype

// -----------------------------------------------------------------------
// definition of class ElemDDLSGOptionResetOption
// -----------------------------------------------------------------------
class ElemDDLSGOptionResetOption : public ElemDDLSGOption {
 public:
  // constructor
  ElemDDLSGOptionResetOption();

  // virtual destructor
  virtual ~ElemDDLSGOptionResetOption();

  // cast
  virtual ElemDDLSGOptionResetOption *castToElemDDLSGOptionResetOption();

  // methods for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString getText() const;

  // method for building text
  virtual NAString getSyntax() const;

 private:
};  // class ElemDDLSGOptionResetOption

// -----------------------------------------------------------------------
// definition of class ElemDDLSGOptionNextValOption
// -----------------------------------------------------------------------
class ElemDDLSGOptionNextValOption : public ElemDDLSGOption {
 public:
  // constructor
  ElemDDLSGOptionNextValOption(long nextVal);

  // virtual destructor
  virtual ~ElemDDLSGOptionNextValOption();

  // cast
  virtual ElemDDLSGOptionNextValOption *castToElemDDLSGOptionNextValOption();

  // accessors
  inline long getNextVal() const { return nextVal_; };

  // methods for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString getText() const;

  // method for building text
  virtual NAString getSyntax() const;

 private:
  //
  // data member(s)
  //
  long nextVal_;

};  // class ElemDDLSGOptionNextValOption

// -----------------------------------------------------------------------
// definitions of inline methods for class ElemDDLSGOptionStartValue
// -----------------------------------------------------------------------

inline long ElemDDLSGOptionStartValue::getValue() const { return value_; }

// -----------------------------------------------------------------------
// definitions of inline methods for class ElemDDLSGOptionRestartValue
// -----------------------------------------------------------------------

inline long ElemDDLSGOptionRestartValue::getValue() const { return value_; }

// -----------------------------------------------------------------------
// definitions of inline methods for class ElemDDLSGOptionIncrement
// -----------------------------------------------------------------------

inline long ElemDDLSGOptionIncrement::getValue() const { return value_; }

// -----------------------------------------------------------------------
// definitions of inline methods for class ElemDDLSGOptionMaxValue
// -----------------------------------------------------------------------

inline long ElemDDLSGOptionMaxValue::getValue() const { return value_; }

inline NABoolean ElemDDLSGOptionMaxValue::isNoMaxValue() const { return isNoMaxValue_; }

// -----------------------------------------------------------------------
// definitions of inline methods for class ElemDDLSGOptionMinValue
// -----------------------------------------------------------------------

inline long ElemDDLSGOptionMinValue::getValue() const { return value_; }

inline NABoolean ElemDDLSGOptionMinValue::isNoMinValue() const { return isNoMinValue_; }

#endif  // ELEMDDLSGOPTION_H
