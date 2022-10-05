// **********************************************************************

// **********************************************************************

#ifndef _COMMEMLOG_H_
#define _COMMEMLOG_H_

#include <limits.h>
//#include "common/Platform.h"

#define DEFAULT_MEMLOG_SIZE 10485760  // 1024*1024*10

class ComMemLog {
 public:
  ComMemLog();
  virtual ~ComMemLog() { deinitialize(); }
  int initialize(long pid, bool read = true);
  static ComMemLog &instance();
  void memlog(const char *format, ...);
  void memshow();

 private:
  void initForRead();
  void initForWrite();
  void deinitialize();
  void buildPath(long pid);
  void setSize();
  int doMap();
  void loopback();
  void updateCurrentLength(long len);
  // Copy constructor and assignment operator are not defined.
  ComMemLog(const ComMemLog &);
  ComMemLog &operator=(const ComMemLog &);

 private:
  char *startPtr;
  long maxLength;
  long currLength;
  long loopbackNm;
  char fileName[NAME_MAX];
  int fileHandle;
  bool isRead;
};

#endif /* _COMMEMLOG_H_ */
