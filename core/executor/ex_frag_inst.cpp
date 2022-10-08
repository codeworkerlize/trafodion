
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ex_frag_inst.C
 * Description:  Identifiers for fragments and fragment instances
 *
 *
 * Created:      1/24/96
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */

// -----------------------------------------------------------------------

#include "executor/ex_frag_inst.h"

#include "executor/Ex_esp_msg.h"
#include "common/ExCollections.h"
#include "common/Ipc.h"

ExFragKey::ExFragKey()
    : IpcMessageObj(ESP_FRAGMENT_KEY, CurrFragmentKeyVersion),
      statementHandle_(0),
      fragId_(0),
      spare1_(0),
      spare2_(0) {}

ExFragKey::ExFragKey(IpcProcessId pid, ExEspStatementHandle statementHandle, ExFragId fragId)
    : IpcMessageObj(ESP_FRAGMENT_KEY, CurrFragmentKeyVersion),
      pid_(pid),
      statementHandle_(statementHandle),
      fragId_(fragId),
      spare1_(0),
      spare2_(0) {}

ExFragKey::ExFragKey(const ExFragKey &other)
    : IpcMessageObj(ESP_FRAGMENT_KEY, CurrFragmentKeyVersion),
      pid_(other.pid_),
      statementHandle_(other.statementHandle_),
      fragId_(other.fragId_),
      spare1_(0),
      spare2_(0) {}

NABoolean ExFragKey::operator==(const ExFragKey &other) {
  return (other.fragId_ == fragId_ AND other.statementHandle_ == statementHandle_ AND other.pid_ == pid_);
}

IpcMessageObjSize ExFragKey::packedLength() { return sizeof(ExFragKey); }
