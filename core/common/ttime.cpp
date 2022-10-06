
//
#include "ttime.h"

#include <math.h>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>

#include "common/ComCextdecs.h"

const char *reportTimestamp() {
  char buffer[26];
  int millisec;
  struct tm *tm_info;
  struct timeval tv;

  gettimeofday(&tv, NULL);

  millisec = lrint(tv.tv_usec / 1000.0);  // Round to nearest millisec
  if (millisec >= 1000) {                 // Allow for rounding up to nearest second
    millisec -= 1000;
    tv.tv_sec++;
  }

  tm_info = localtime(&tv.tv_sec);

  strftime(buffer, 26, "%Y:%m:%d %H:%M:%S", tm_info);

  static THREAD_P char buf[100];
  sprintf(buf, "%s.%03d, ", buffer, millisec);

  return buf;
}

const char *reportTimeDiff(long time) {
  static THREAD_P char tdiff[200];
  int ms = (int)(((time % 1000000) + 500) / 1000);
  int sec = (int)(time / 1000000);
  int min = sec / 60;
  sec = sec % 60;
  int hour = min / 60;
  min = min % 60;

  sprintf(tdiff, "%02u:%02u:%02u.%03u (us=%ld)", hour, min, sec, ms, time);

  return tdiff;
}

long currentTimeStampInUsec() {
  struct timeval tv;

  gettimeofday(&tv, NULL);

  return (long)(tv.tv_sec * 1000000) + tv.tv_usec;
}
