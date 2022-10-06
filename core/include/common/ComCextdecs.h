
#ifndef COMCEXTDECS_H
#define COMCEXTDECS_H

#include "common/ComCextMisc.h"
#include "common/Platform.h"
#include "common/cextdecs.h"

#define NA_JulianTimestamp()          JULIANTIMESTAMP(0, 0, 0, -1)
#define NA_ConvertTimestamp(jTMStamp) CONVERTTIMESTAMP(jTMStamp, 0, -1, 0)

/*  Some common wrappers  */
#define NA_InterpretTimestamp(jtm, tm)   INTERPRETTIMESTAMP(jtm, tm)
#define NA_ComputeTimestamp(tmStmp, err) COMPUTETIMESTAMP(tmStmp, err)

#endif /*  COMCEXTDECS_H  */
