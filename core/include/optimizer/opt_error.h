#ifndef OPT_ERROR_H
#define OPT_ERROR_H
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         opt_error.h
 * Description:  Binder/normalizer/optimizer error codes
 *

 *
 *****************************************************************************
 */

enum OptimizerSQLErrorCode {
  BIND_CONTROL_QUERY_SUCCESSFUL = 4074,

  CATALOG_HISTOGRM_HISTINTS_TABLES_CONTAIN_BAD_VALUES = -6002,
  CATALOG_HISTOGRM_HISTINTS_TABLES_CONTAIN_BAD_VALUE = 6003,
  CATALOG_HISTINTS_TABLES_CONTAIN_BAD_VALUES = 6004,
  MULTI_COLUMN_STATS_NEEDED = 6007,
  SINGLE_COLUMN_STATS_NEEDED = 6008,
  OSIM_ERRORORWARNING = 6009,
  MULTI_COLUMN_STATS_NEEDED_AUTO = 6010,
  SINGLE_COLUMN_STATS_NEEDED_AUTO = 6011,
  SINGLE_COLUMN_SMALL_STATS = 6012,
  SINGLE_COLUMN_SMALL_STATS_AUTO = 6013,
  CMP_OPT_WARN_FROM_DCMPASSERT = 6021
};

#endif /* OPT_ERROR_H */
