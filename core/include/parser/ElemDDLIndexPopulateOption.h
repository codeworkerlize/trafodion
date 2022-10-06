
#ifndef ELEMDDLINDEXPOPULATEOPTION_H
#define ELEMDDLINDEXPOPULATEOPTION_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ElemDDLIndexPopulateoption.h
 * Description:  class for Create Index with Populate and No
 *               Populate statement. (parser node)
 *
 *
 * Created:      05/29/98
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

class ElemDDLIndexPopulateOption : public ElemDDLNode {
 public:
  // constructor for the populate option.
  // The constructor set the option default to be populate, and the check to see if the populate and
  // no populate both exist in one statement. Error will be issue if both populate and no populate
  // clause exist.
  ElemDDLIndexPopulateOption()
      : ElemDDLNode(ELM_WITH_POPULATE_OPTION_ELEM)

  {
    populateCount_ = 0;
    noPopulateCount_ = 0;
    populateOption_ = TRUE;
    noPopulateOption_ = FALSE;
  }

  // return 1 if populate is specified. Otherwise, return 0.
  ComBoolean getPopulateOption() { return populateOption_; };

  // return 1 if no populate is specified . Otherwise, return 0.
  ComBoolean getNoPopulateOption() { return noPopulateOption_; };

  // chekck to see if the populate clause was specified more than 1.
  int getPopulateOptionCount() { return populateCount_; };

  // check to see if the no populate clause was specified more than 1.
  int getNoPopulateOptionCount() { return noPopulateCount_; };

  // set populate option counter, should not be more than 1.
  void setPopulateOptionCount() { populateCount_++; };

  // set no populate option counter, should not be more than 1.
  void setNoPopulateOptionCount() { noPopulateCount_++; };

  // specified the no populate clause.
  void setNoPopulateClause(ComBoolean option) { noPopulateOption_ = option; };

  // specified the populate clause.
  void setPopulateClause(ComBoolean option) { populateOption_ = option; };

  // cast virtual function.
  virtual ElemDDLIndexPopulateOption *castToElemDDLIndexPopulateOption() { return this; };

 private:
  ComBoolean populateOption_;
  ComBoolean noPopulateOption_;
  int populateCount_;
  int noPopulateCount_;
};

#endif  // ELEMDDLINDEXPOPULATEOPTION_H
