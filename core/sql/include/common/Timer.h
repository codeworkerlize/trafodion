
#ifndef TIMER_H
#define TIMER_H

#include "common/ComCextdecs.h"

class Timer {
 private:
  enum TimerConstants { ONE_THOUSAND = 1000, ONE_MILLION = 1000000 };

  long startTime_;
  long endTime_;
  long accumTime_;
  NABoolean running_;

 public:
  // A timer needs to be explicitly started using 'start' or 'restart'
  Timer() : running_(false), startTime_(0), accumTime_(0) {}

  NABoolean start() {
    // Return immediately if the timer is already running
    if (!running_) {
      // Set timer status to running and set the start time
      running_ = TRUE;
      accumTime_ = 0;
      startTime_ = NA_JulianTimestamp();
    }

    return running_;
  }

  //
  // If timer is running, then stop the timer, accumulate any elapsed time.
  // Returns accumulated time.
  long stop() {
    if (running_) {
      endTime_ = NA_JulianTimestamp();
      running_ = FALSE;
      accumTime_ += endTime_ - startTime_;
    }

    return accumTime_;
  }

  //
  // If timer is not running, then set to running and capture a new start time.
  // Returns current accumulated time.
  long restart() {
    if (!running_) {
      running_ = TRUE;
      endTime_ = 0;
      startTime_ = NA_JulianTimestamp();
      // accumTime_   this is incremented when stop is called
    }
    return accumTime_;
  }

  //
  // If timer is running, then stop it and accumulate elapsed time.
  // If timer is not running, then start it.
  // Returns current accumulated time.
  long startStop() { return (running_ ? stop() : restart()); }

  //
  // If timer is running, then return the accumulated time so far plus
  // the current time minus the current start time.
  // If timer is not running, then just return the accumulated time.
  long elapsedTime() {
    if (running_)
      return ((NA_JulianTimestamp() - startTime_) + accumTime_);
    else {
      return (accumTime_);
    }
  }
};

#endif  // TIMER_H
