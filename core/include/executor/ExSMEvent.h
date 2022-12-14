
#ifndef EXSM_EVENT_H
#define EXSM_EVENT_H

#include <time.h>

#include "seabed/sm.h"

// The ExSMEvent class serves two purposes:
//
// * Static member functions provide an interface for writing to an
//   in-memory trace
//
// * The class also uses non-static data members to act as a container
// * for a single trace event
//
class ExSMEvent {
  ExSMEvent() {}
  virtual ~ExSMEvent() {}

 public:
  // Data members for a single trace event
  struct timespec ts_;
  char fn_[5];
  sm_target_t target_;
  int64_t optional_[5];

  // Character labels for the different event types
  static const char *Init;
  static const char *Finalize;
  static const char *Register;
  static const char *Cancel;
  static const char *Send;
  static const char *SendRetry;
  static const char *Post;
  static const char *Receive;
  static const char *Chunk;
  static const char *IDNotActive;
  static const char *ReceiveDone;
  static const char *SMError;
  static const char *ControlConnectionError;
  static const char *Exit;

  // Function to add an event to the trace
  static ExSMEvent *add(const char *fn, const sm_target_t *target = NULL, int64_t i1 = 0, int64_t i2 = 0,
                        int64_t i3 = 0, int64_t i4 = 0, int64_t i5 = 0);

  // Functions to be called in each thread to initialize in-memory
  // trace structures for that thread
  static void initMainThread();
  static void initReaderThread();

  // Functions to print the trace to stdout
  static void printMainThread();
  static void printReaderThread();

  // Functions to update the optional integers in a trace event
  void setOptional1(int64_t i) { optional_[0] = i; }
  void setOptional2(int64_t i) { optional_[1] = i; }
  void setOptional3(int64_t i) { optional_[2] = i; }
  void setOptional4(int64_t i) { optional_[3] = i; }
  void setOptional5(int64_t i) { optional_[4] = i; }
};

#endif
