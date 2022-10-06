//------------------------------------------------------------------
//

//
//  XARM Header file
//-------------------------------------------------------------------

#ifndef XARM_H_
#define XARM_H_
#include "dtm/xa.h"

// Globals

#define XARM_RM_NAME "SEAQUEST_RM_XARM"

// Default to XARM. xaTM_initialize will set to tse_switch  for TMs.
extern xa_switch_t xarm_switch;

// XARM generic interface to DTM
extern int xarm_xa_close(char *, int, int64);
extern int xarm_xa_commit(XID *, int, int64);
extern int xarm_xa_complete(int *, int *, int, int64);
extern int xarm_xa_end(XID *, int, int64);
extern int xarm_xa_forget(XID *, int, int64);
extern int xarm_xa_open(char *, int, int64);
extern int xarm_xa_prepare(XID *, int, int64);
extern int xarm_xa_recover(XID *, int64, int, int64);
extern int xarm_xa_rollback(XID *, int, int64);
extern int xarm_xa_start(XID *, int, int64);
#endif  // XARM_H_
