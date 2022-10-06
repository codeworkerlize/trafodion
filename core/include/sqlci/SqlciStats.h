
#ifndef SQLCI_STATS_H
#define SQLCI_STATS_H

/* -*-C++-*-
 *****************************************************************************
 *
 * File:         SqlciStats.h
 * RCS:          $Id: SqlciStats.h,v 1.2.18.1 1998/03/11 22:34:26  Exp $
 * Description:
 *
 *
 * Created:      7/25/95
 * Modified:     $ $Date: 1998/03/11 22:34:26 $ (GMT)
 * Language:     C++
 * Status:       $State: Exp $
 *
 *
 *
 *
 *****************************************************************************
 */

#include <sys/time.h>

#include "common/Platform.h"
#include "sqlci/SqlciCmd.h"  // Statistics class enum

class SqlciEnv;

class SqlciStats {
  enum StatsStatus { NO_STATS, STATS_AVAILABLE };

  // to keep track of the statement whose statistics are being gathered.
  // Used at "display statistics" time to retrieve information from
  // executor via cli calls.
  PrepStmt *prep_stmt_;

  timeval start_time;
  timeval end_time;

  // to keep track of SQL execution time.
  timeval exe_start_time;
  timeval exe_end_time;

  // if set to -1, indicates that a query is in progress.
  StatsStatus statsStatus_;

  // statistics to be displayed after each query.
  // Set to ON by SET STATISTICS ON.
  NABoolean statsDisplay_;

  char *statsOptions_;

 public:
  SqlciStats();
  ~SqlciStats();

  void startStats(PrepStmt *prep_stmt = 0);
  void endStats(SqlciEnv *sqlci_env);

  void startExeStats();
  void endExeStats();

  short displayStats(SqlciEnv *sqlci_env);
  short displayChildQryStats(SqlciEnv *sqlci_env);

  inline void setStatsDisplay(NABoolean stats_disp) { statsDisplay_ = stats_disp; };

  void setStatsOptions(char *statsOptions) {
    if (statsOptions_) delete statsOptions_;

    if (statsOptions) {
      statsOptions_ = new char[strlen(statsOptions) + 1];
      strcpy(statsOptions_, statsOptions);
    } else
      statsOptions_ = NULL;
  }

  NABoolean getStatsDisplay() { return statsDisplay_; };
};

#endif
