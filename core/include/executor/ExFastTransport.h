

#ifndef __EX_FAST_TRANSPORT_H
#define __EX_FAST_TRANSPORT_H

#include "common/ComSmallDefs.h"
#include "executor/ex_exe_stmt_globals.h"
#include "executor/ExStats.h"
#include "executor/ex_tcb.h"
#include "exp/ExpLOBinterface.h"

namespace {
typedef std::vector<Text> TextVec;
}

// -----------------------------------------------------------------------
// Forward class declarations
// -----------------------------------------------------------------------
class SequenceFileWriter;

// -----------------------------------------------------------------------
// Classes defined in this file
// -----------------------------------------------------------------------

class IOBuffer {
 public:
  enum BufferStatus { PARTIAL = 0, FULL, EMPTY, ERR };

  IOBuffer(char *buffer, int bufSize) : bytesLeft_(bufSize), bufSize_(bufSize), numRows_(0), status_(EMPTY) {
    data_ = buffer;
    // memset(data_, '\0', bufSize);
  }

  ~IOBuffer() {}

  void setStatus(BufferStatus val) { status_ = val; }
  BufferStatus getStatus() { return status_; }

  char *data_;
  int bytesLeft_;
  int bufSize_;
  int numRows_;
  BufferStatus status_;
};
//----------------------------------------------------------------------
// Task control block

#endif  // __EX_FAST_TRANSPORT_H
