//------------------------------------------------------------------
//

#ifndef __SB_INT_OPTS_H_
#define __SB_INT_OPTS_H_

#define USE_NOWAIT_OPEN      1  // can't take out - due to SQL dependency
#define USE_NEW_START_NOWAIT 1  // can't take out - due to SQL dependency
#define USE_SB_UTRACE_API       // micro trace-api
#define USE_SB_NEW_RI           // receive-info

#ifdef SQ_PHANDLE_VERIFIER
#ifndef USE_PHAN_VERIFIER
#define USE_PHAN_VERIFIER
#endif
#endif

// post-process defines
#ifdef USE_SB_INLINE
#define SB_INLINE inline
#else
#define SB_INLINE
#endif
#ifdef USE_SB_MAP_STATS
#ifndef USE_SB_NO_SL
#define USE_SB_NO_SL
#endif
#endif

#endif  // !__SB_INT_OPTS_H_
