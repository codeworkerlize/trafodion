
#ifndef ELEMDDLLOADOPTIONS_H
#define ELEMDDLLOADOPTIONS_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLLoadOptions.h
 * Description:  classes for load options specified in DDL statements
 *
 *
 * Created:      9/28/95
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

#include "common/BaseTypes.h"
#include "ElemDDLNode.h"

// -----------------------------------------------------------------------
// contents of this file
// -----------------------------------------------------------------------
class ElemDDLLoadOpt;
class ElemDDLLoadOptDSlack;
class ElemDDLLoadOptISlack;

// -----------------------------------------------------------------------
// forward references
// -----------------------------------------------------------------------
// None

// -----------------------------------------------------------------------
// definition of base class ElemDDLLoadOpt
// -----------------------------------------------------------------------
class ElemDDLLoadOpt : public ElemDDLNode {
 public:
  // default constructor
  ElemDDLLoadOpt(OperatorTypeEnum operatorType = ELM_ANY_LOAD_OPT_ELEM) : ElemDDLNode(operatorType) {}

  // virtual destructor
  virtual ~ElemDDLLoadOpt();

  // cast
  virtual ElemDDLLoadOpt *castToElemDDLLoadOpt();

  // method for tracing
  virtual const NAString getText() const;

 private:
};  // class ElemDDLLoadOpt

// -----------------------------------------------------------------------
// definition of class ElemDDLLoadOptDSlack
// -----------------------------------------------------------------------
class ElemDDLLoadOptDSlack : public ElemDDLLoadOpt {
 public:
  enum { DEFAULT_PERCENTAGE = 15 };

  // constructor
  ElemDDLLoadOptDSlack(int percentage);

  // virtual destructor
  virtual ~ElemDDLLoadOptDSlack();

  // cast
  virtual ElemDDLLoadOptDSlack *castToElemDDLLoadOptDSlack();

  // accessors
  inline unsigned short getPercentage() const;

  // methods for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString getText() const;

  // method for building text
  virtual NAString getSyntax() const;

 private:
  //
  // method(s)
  //

  NABoolean isLegalPercentageValue(int percentage);

  //
  // data member(s)
  //

  unsigned short percentage_;

};  // class ElemDDLLoadOptDSlack

// -----------------------------------------------------------------------
// definition of class ElemDDLLoadOptISlack
// -----------------------------------------------------------------------
class ElemDDLLoadOptISlack : public ElemDDLLoadOpt {
 public:
  enum { DEFAULT_PERCENTAGE = 15 };

  // constructor
  ElemDDLLoadOptISlack(int percentage);

  // virtual destructor
  virtual ~ElemDDLLoadOptISlack();

  // cast
  virtual ElemDDLLoadOptISlack *castToElemDDLLoadOptISlack();

  // accessors
  inline unsigned short getPercentage() const;

  // methods for tracing
  virtual const NAString displayLabel1() const;
  virtual const NAString getText() const;

  // method for building text
  virtual NAString getSyntax() const;

 private:
  //
  // method(s)
  //

  NABoolean isLegalPercentageValue(int percentage);

  //
  // data member(s)
  //

  unsigned short percentage_;

};  // class ElemDDLLoadOptISlack

// -----------------------------------------------------------------------
// definitions of inline methods for class ElemDDLLoadOptDSlack
// -----------------------------------------------------------------------

inline unsigned short ElemDDLLoadOptDSlack::getPercentage() const { return percentage_; }

// -----------------------------------------------------------------------
// definitions of inline methods for class ElemDDLLoadOptISlack
// -----------------------------------------------------------------------

inline unsigned short ElemDDLLoadOptISlack::getPercentage() const { return percentage_; }

#endif  // ELEMDDLLOADOPTIONS_H
