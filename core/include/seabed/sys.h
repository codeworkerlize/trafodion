//------------------------------------------------------------------
//


#ifndef __SB_SYS_H_
#define __SB_SYS_H_

#include <pthread.h>
#include <unistd.h>
#include <linux/unistd.h>  // gettid

extern __thread pid_t gCurThreadId;
static inline void init_tid() {
  if (gCurThreadId < 0) gCurThreadId = (pid_t)syscall(__NR_gettid);
}
#define GETTID() (init_tid(), gCurThreadId)

#endif  //!__SB_SYS_H_
