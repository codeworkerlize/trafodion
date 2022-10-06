
#ifndef EX_FRAG_INST_H
#define EX_FRAG_INST_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ex_frag_inst.h
 * Description:  Identifiers for fragments and fragment instances
 *
 *
 * Created:      1/22/96
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

// -----------------------------------------------------------------------

#include "comexe/FragDir.h"
#include "common/Ipc.h"
#include "export/NAVersionedObject.h"

// -----------------------------------------------------------------------
// Contents of this file
// -----------------------------------------------------------------------

class ExFragKey;
// typedef int;
// typedef ExEspStatementHandle;

// -----------------------------------------------------------------------
// A fragment instance handle is a unique identifier for a downloaded
// fragment instance within its ESP.
// -----------------------------------------------------------------------

// typedef CollIndex int;
const int NullFragInstanceHandle = NULL_COLL_INDEX;

// -----------------------------------------------------------------------
// A statement handle identifies a statement in a given process
// -----------------------------------------------------------------------
typedef int ExEspStatementHandle;

// -----------------------------------------------------------------------
// A fragment key is a unique identifier for a fragment of a particular
// statement in a particular process. Given to an ESP, the ESP can find
// the downloaded fragment instance. This object is copied into messages.
// A fragment key consists of
// - the process id of the master executor
// - the statement handle of the master executor
// - the fragment id
// Together, those three values uniquely identify one fragment of one
// statement in one executing SQL program. Note that multiple instances
// of this fragment may exist in multiple ESPs, but each ESP has at most
// one instance.
// -----------------------------------------------------------------------
class ExFragKey : public IpcMessageObj {
 public:
  ExFragKey();
  ExFragKey(IpcProcessId pid, ExEspStatementHandle statementHandle, ExFragId fragId);
  ExFragKey(const ExFragKey &other);
  NABoolean operator==(const ExFragKey &other);

  inline const IpcProcessId &getProcessId() const { return pid_; }
  inline ExEspStatementHandle getStatementHandle() const { return statementHandle_; }
  inline ExFragId getFragId() const { return fragId_; }
  inline void setFragId(ExFragId fid) { fragId_ = fid; }

  IpcMessageObjSize packedLength();

 private:
  IpcProcessId pid_;
  ExEspStatementHandle statementHandle_;
  ExFragId fragId_;
  int spare1_;
  int spare2_;
};

#endif /* EX_FRAG_INST_H */
