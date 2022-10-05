
/* -*-C++-*-
 *****************************************************************************
 *
 * File:        sq_sql_events.h
 * Description:  Event Ids for SQL
 *
 * Created:      1105/2010
 * Language:     C++
 *
 *
 *
 *
 ****************************************************************************/
// This file contains the event ids for SQL informational events.
// Any new event ids for new events  must be added to this file and
// must be unique
// We are preserving the sevent numbers from Neo for relevant events
// But when they appear in sealog, they will have 9 digits of the form
// 109000<event id>
// eg if event id is 500, then it will appear as 109000500
// if event id is 2000 then it will appear as 109002000

#define SQEV_SQL_ABORT               501
#define SQEV_SQL_ASSERTION_FAILURE   502
#define SQEV_SQL_OPT_PASS1_FAILURE   503
#define SQEV_SQL_OPT_PASS2_FAILURE   504
#define SQEV_SQL_CLI_RECLAIM_OCCURED 506
#define SQEV_SQL_SRT_INFO            507
#define SQEV_SQL_EXEC_RT_INFO        508
#define SQEV_CMP_NQC_RETRY_OCCURED   514

// QVP
#define SQEV_QVP_INFO    590
#define SQEV_QVP_ERROR   591
#define SQEV_QVP_FAILURE 592

// MVQR
#define SQEV_MVQR_INFO    596
#define SQEV_MVQR_ERROR   597
#define SQEV_MVQR_FAILURE 598

// SQL debugging aids for error events
#define SQEV_SQL_DEBUG_EVENT 600

// Mv refresh
#define SQEV_MVREFRESH_INFO  602
#define SQEV_MVREFRESH_ERROR 603
