
#ifndef EXP_LOB_STATS_H
#define EXP_LOB_STATS_H

#include "common/Platform.h"

#define NUM_NSECS_IN_SEC (1 * 1000 * 1000 * 1000)

class ExLobStats {
 public:
  void init();
  ExLobStats &operator+(const ExLobStats &other);
  void getVariableStatsInfo(char *dataBuffer, char *datalen, int maxLen);

  long bytesToRead;
  long bytesRead;
  long bytesWritten;
  long hdfsConnectionTime;
  long CumulativeReadTime;
  long hdfsAccessLayerTime;
  long cursorElapsedTime;
  long CumulativeWriteTime;
  long AvgReadTime;
  long AvgWriteTime;
  long numBufferPrefetchFail;
  long avgReqQueueSize;
  long numReadReqs;
  long numWriteReqs;
  long numHdfsReqs;
  long bytesPrefetched;
  long buffersUsed;
};

#endif
