
#ifndef _RU_MV_EQUIVSET_BUILDER_H_
#define _RU_MV_EQUIVSET_BUILDER_H_

/* -*-C++-*-
******************************************************************************
*
* File:         RuMVEquivSetBuilder.h
* Description:  Definition of class CRUMVEquivSetBuilder
*
* Created:      11/23/2000
* Language:     C++
*
*
******************************************************************************
*/

#include "refresh.h"

#include "RuMV.h"
#include "RuEquivSetBuilder.h"

//--------------------------------------------------------------------------//
// CRUMVEquivSetBuilder
//
//	The input for this class is a set of the involved MVs, and the output
//	is a list of root MVs for each equivalence set.
//
//	An MV equivalence set contains involved ON REQUEST MVs using each other,
//	either directly or indirectly. The *root MV* in the set is the MV that
//	uses no other MV.
//
//--------------------------------------------------------------------------//

class REFRESH_LIB_CLASS CRUMVEquivSetBuilder : public CRUEquivSetBuilder {
  //----------------------------------//
  //	Public Members
  //----------------------------------//
 public:
  CRUMVEquivSetBuilder();

  virtual ~CRUMVEquivSetBuilder();

 public:
  // This function is used for entering the input
  // for the builder
  // The whole graph is build from the given mv's
  virtual void AddMV(CRUMV *pMV);

  CRUMVList &GetSet(int num);

  // The main execution function for this class
  virtual void Run();

  // These functions are used for retrieving the results
  virtual int GetNumOfSets() { return equivSetsRootsList_.GetCount(); }

  CRUMVList &GetEquivSetsRootsByMVUID(TInt64 uid);

 public:
#ifdef _DEBUG
  // For debug purpose , dumps all sets to the output file
  virtual void DumpSets();
#endif

  //----------------------------------//
  //	Private Members
  //----------------------------------//

  // Run() callee
 private:
  void BuildDisJointGraph();
  void BuildSets();
  void AddOnRequestMVs();
  void AddOnRequestMV(CRUMV *pMV);

 private:
  CRUMVList onRequestMVsList_;

  CDSPtrList<CRUMVList> equivSetsRootsList_;

  CDSPtrList<CRUMV> rootsMvsList_;
};

#endif
