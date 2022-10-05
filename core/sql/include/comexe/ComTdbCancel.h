/**********************************************************************

// File:         ComTdbCancel.h
// Description:  Class declaration for ComTdbCancel.
//
// Created:      Oct 15, 2009
**********************************************************************/
#ifndef COM_CANCEL_H
#define COM_CANCEL_H

#include "comexe/ComTdb.h"

//
// Task Definition Block
//
class ComTdbCancel : public ComTdb {
  friend class ExCancelTcb;

 protected:
  NABasicPtr qid_;                 // 00-07
  Int16 action_;                   // 08-09
  Int16 forced_;                   // 10-11
  int cancelPidBlockThreshold_;  // 12-15
  NABasicPtr comment_;             // 16-23
  NABasicPtr cancelPname_;         // 24-31
  int cancelNid_;                // 32-35
  int cancelPid_;                // 36-39
  char fillersComTdbCancel2_[24];  // 40-63

 public:
  enum Action { CancelByQid, CancelByPname, CancelByNidPid, Suspend, Activate, InvalidAction };

  enum ForceOption { Safe, Force, CancelOrActivateIsAlwaysSafe };

  // Constructor
  ComTdbCancel();  // dummy constructor. Used by 'unpack' routines.

  ComTdbCancel(char *qid, char *pname, int nid, int pid, int minAge, Int16 action, Int16 forced, char *comment,
               ex_cri_desc *given_cri_desc, ex_cri_desc *returned_cri_desc, queue_index down, queue_index up);

  // ---------------------------------------------------------------------
  // Redefine virtual functions required for Versioning.
  //----------------------------------------------------------------------
  virtual unsigned char getClassVersionID() { return 1; }

  virtual void populateImageVersionIDArray() {
    setImageVersionID(1, getClassVersionID());
    ComTdb::populateImageVersionIDArray();
  }

  virtual short getClassSize() { return (short)sizeof(ComTdbCancel); }
  Long pack(void *);

  int unpack(void *, void *reallocator);

  void display() const;

  inline ComTdb *getChildTdb();

  // ---------------------------------------------------------------------
  // Used by the internal SHOWPLAN command to get attributes of a TDB.
  // ---------------------------------------------------------------------
  virtual void displayContents(Space *space, int flag);

  virtual const ComTdb *getChild(int pos) const;

  virtual int numChildren() const { return 0; }

  virtual const char *getNodeName() const { return "EX_CANCEL"; };

  virtual int numExpressions() const { return 0; }

  inline const char *getQidText() const { return qid_; }

  inline bool actionIsCancel() const {
    return (action_ == CancelByQid) || (action_ == CancelByPname) || (action_ == CancelByNidPid);
  }

  inline Action getAction() const { return (Action)action_; }

  inline ForceOption getForce() const { return (ForceOption)forced_; }

  inline char *getCommentText() const { return comment_; }

  inline char *getCancelPname() const { return cancelPname_; }

  inline int getCancelNid() const { return cancelNid_; }

  inline int getCancelPid() const { return cancelPid_; }

  inline int getCancelPidBlockThreshold() const { return cancelPidBlockThreshold_; }
};

inline ComTdb *ComTdbCancel::getChildTdb() { return NULL; };

/*****************************************************************************
  Description : Return ComTdb* depending on the position argument.
                  Position 0 means the left most child.
  Comments    :
  History     : Yeogirl Yun                                      8/22/95
                 Initial Revision.
*****************************************************************************/
inline const ComTdb *ComTdbCancel::getChild(int pos) const { return NULL; }

#endif
