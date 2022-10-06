
#ifndef __MEASURE_H_
#define __MEASURE_H_

#undef MEASURE

#ifdef MEASURE

#include <sys/times.h>
#include <stdio.h>

class LogAgent {
 public:
  LogAgent();
  ~LogAgent();
};

class Timer {
 public:
  void timerOn();
  void timerOff();
  void timerLog(const char *msg, const char *file, int line);

  static void openLog();
  static void closeLog();
  static void flushLog();

 private:
  clock_t startClock;
  clock_t endClock;
  tms startTms;
  tms endTms;
  static FILE *logFile;
};

#define LOG_AGENT LogAgent logAgent;

#define LOG_FLUSH Timer::flushLog();

#define TIMER_ON(timerId) \
  Timer timerId;          \
  timerId.timerOn();

#define TIMER_OFF(timerId, msg) \
  timerId.timerOff();           \
  timerId.timerLog(msg, __FILE__, __LINE__);

#else  // #ifdef MEASURE

#define LOG_AGENT
#define TIMER_ON(timerId)
#define TIMER_OFF(timerId, msg)
#define LOG_FLUSH

#endif  // #ifdef MEASURE

#endif  // __MEASURE_H
