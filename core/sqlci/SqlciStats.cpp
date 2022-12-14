

#include "sqlci/SqlciStats.h"

#include <stdio.h>
#include <time.h>

#include <iostream>

#include "cli/sql_id.h"
#include "common/ComCextdecs.h"
#include "common/ComSysUtils.h"
#include "common/Platform.h"
#include "common/str.h"
#include "sqlci/Sqlci.h"

short Statistics::process(SqlciEnv *sqlci_env) {
  switch (type_) {
    case SET_ON:
      sqlci_env->getStats()->setStatsDisplay(TRUE);
      sqlci_env->getStats()->setStatsOptions(getStatsOptions());
      break;

    case SET_OFF:
      sqlci_env->getStats()->setStatsDisplay(FALSE);
      break;
  }

  return 0;
}

SqlciStats::SqlciStats() {
  statsStatus_ = NO_STATS;
  statsDisplay_ = FALSE;
  statsOptions_ = NULL;
}

SqlciStats::~SqlciStats() {
  if (statsOptions_) delete[] statsOptions_;
}

void SqlciStats::startStats(PrepStmt *prep_stmt) {
  prep_stmt_ = prep_stmt;

  statsStatus_ = NO_STATS;

  NA_gettimeofday(&start_time, 0);
}

void SqlciStats::endStats(SqlciEnv *sqlci_env) {
  NA_gettimeofday(&end_time, 0);

  statsStatus_ = STATS_AVAILABLE;
}

void SqlciStats::startExeStats() { NA_gettimeofday(&exe_start_time, 0); }

void SqlciStats::endExeStats() { NA_gettimeofday(&exe_end_time, 0); }

short SqlciStats::displayStats(SqlciEnv *sqlci_env) {
  int retcode = 0;

  if (statsStatus_ != STATS_AVAILABLE) return 0;
  // do not display, if stats display is set to off.
  if (statsDisplay_ == FALSE) return 0;
  int newStrLen = strlen("GET STATISTICS ") + (statsOptions_ ? strlen(statsOptions_) : 0) + 50;
  char *newStr = new char[newStrLen + 1];
  NABoolean displayAll = FALSE;
  strcpy(newStr, "GET STATISTICS ");
  if (statsOptions_) {
    if (strcmp(statsOptions_, "PERTABLE") == 0)
      strcat(newStr, "FOR QID CURRENT PERTABLE, OPTIONS 'SL'");
    else if (strcmp(statsOptions_, "PROGRESS") == 0)
      strcat(newStr, "FOR QID CURRENT PROGRESS, OPTIONS 'SL'");
    else if (strcmp(statsOptions_, "DEFAULT") == 0)
      strcat(newStr, "FOR QID CURRENT DEFAULT ");
    else if (strcmp(statsOptions_, "ALL") == 0)
      displayAll = TRUE;
    else if (statsOptions_) {
      strcat(newStr, ", options '");
      strcat(newStr, statsOptions_);
      strcat(newStr, "'");
    }
  }
  strcat(newStr, ";");
  statsDisplay_ = FALSE;
  if (displayAll) {
    strcpy(newStr, "GET STATISTICS FOR QID CURRENT PROGRESS , OPTIONS 'SL'");
    DML dml(newStr, DML_DESCRIBE_TYPE, "__MXCI_GET_STATS__");
    retcode = dml.process(sqlci_env);
    strcpy(newStr, "GET STATISTICS FOR QID CURRENT DEFAULT ;");
    DML dml1(newStr, DML_DESCRIBE_TYPE, "__MXCI_GET_STATS__");
    retcode = dml1.process(sqlci_env);
  } else {
    DML dml(newStr, DML_DESCRIBE_TYPE, "__MXCI_GET_STATS__");
    retcode = dml.process(sqlci_env);
  }
  delete[] newStr;

  statsDisplay_ = TRUE;

  return (short)retcode;
}

short SqlciStats::displayChildQryStats(SqlciEnv *sqlci_env) {
  int retcode = 0;

  // do not display, if stats display is set to off.
  if (statsDisplay_ == FALSE) return 0;

  int newStrLen = strlen("GET STATISTICS FOR QID CURRENT") + 10;
  char *newStr = new char[newStrLen + 1];
  strcpy(newStr, "GET STATISTICS FOR QID CURRENT ;");

  statsDisplay_ = FALSE;
  DML dml(newStr, DML_DESCRIBE_TYPE, "__MXCI_GET_CHILDQRY_STATS__");
  retcode = dml.process(sqlci_env);
  delete[] newStr;

  statsDisplay_ = TRUE;

  return (short)retcode;
}
