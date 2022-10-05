
#ifndef RELFASTTRANSPORT_H
#define RELFASTTRANSPORT_H
/* -*-C++-*-
**************************************************************************
*
* File:         RelFastTransport.h
* Description:  RelExprs related to support of FastTransport
* Created:      09/29/12
* Language:     C++
*
*************************************************************************
*/

#include "optimizer/RelExpr.h"

// -----------------------------------------------------------------------
// Classes defined in this file
// -----------------------------------------------------------------------
class CostMethodFastExtract;

class UnloadOption {
 public:
  enum UnloadOptionType {
    DELIMITER_,
    NULL_STRING_,
    RECORD_SEP_,
    APPEND_,
    HEADER_,
    COMPRESSION_,
    EMPTY_TARGET_,
    LOG_ERRORS_,
    STOP_AFTER_N_ERRORS_,
    NO_OUTPUT_,
    COMPRESS_,
    ONE_FILE_,
    USE_SNAPSHOT_SCAN_
  };
  UnloadOption(UnloadOptionType option, int numericVal, char *stringVal, char *stringVal2 = NULL)
      : option_(option), numericVal_(numericVal), stringVal_(stringVal){};

 private:
  UnloadOptionType option_;
  int numericVal_;
  char *stringVal_;
};

#endif /* RELFASTTRANSPORT_H */
