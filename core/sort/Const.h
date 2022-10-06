
#ifndef CONST_H
#define CONST_H

#include "common/Platform.h"
#include "common/BaseTypes.h"

const int ONE_MB = 1048576;  // 1024 * 1024

const int SORT_SUCCESS = 0;
const int SORT_FAILURE = 1;
const int SORT_IO_IN_PROGRESS = 2;

const int SORT_MERGENODE_NUM_BUFFERS = 2;

const short REPL_SELECT = 1;
const short QUICKSORT = 2;
const short ITER_QUICKSORT = 3;
const int SCRATCH_BLOCK_SIZE = 56 * 1024;
const int MAXSCRFILES = 4096;  // Must be equal to MAXRUNS
const int FILENAMELEN = 48;    // For NSK
const int MAX_PATH_LEN = 256;  // For NT
const int MAXRUNS = 4096;
const int OVERHEAD = 20;  // The overhead for Scratch Buffer header struct
const int MAX_SCRATCH_FILE_OPENS = 4;

// These extent sizes are recommende dby DP2 in
// support for setmode(141,5) and setmode(141,9) and setmode(141,11)options.
const int PRIMARY_EXTENT_SIZE = 32452;
const int SECONDARY_EXTENT_SIZE = 32788;
const int MAX_EXTENTS = 16;
const int SCRATCH_FILE_SIZE = 2147483647;  // for NT, UNIX, 2GB

// For setmode(141,11), dp2 requires last 8 bytes of 56kb block free.
// Dp2 recommend using setmode(141,11) instead of setmode(141,9) for performance
const int DP2_CHECKSUM_BYTES = 8;

const short KEYS_ARE_EQUAL = 0;
const short KEY1_IS_SMALLER = -1;
const short KEY1_IS_GREATER = 1;

typedef int SBN;
const int TRUE_L = 1;
const int FALSE_L = 0;

const int MAX_ALLOC_SIZE = 127 * 1024 * 1024;

enum RESULT {
  SCRATCH_SUCCESS = 0,
  SCRATCH_FAILURE = 1,
  IO_NOT_COMPLETE,
  PREVIOUS_FAIL,
  DISK_FULL,
  FILE_FULL,
  READ_EOF,
  WRITE_EOF,
  IO_COMPLETE,
  NEGATIVE_SEEK,
  END_OF_RUN,
  OTHER_ERROR
};

enum SORT_STATE {
  SORT_INIT = 0,
  SORT_SEND = 1,
  SORT_SEND_END = 2,
  SORT_RECEIVE,
  SORT_MERGE,
  SORT_INTERMEDIATE_MERGE,
  SORT_FINAL_MERGE,
  SORT_END
};

#endif
