
#ifndef COMMVDEFS_H
#define COMMVDEFS_H

/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ComMvDefs.h
 * Description:  Small definitions are declared here that are used throughout
 *               the SQL/ARK product.
 *
 * Created:      07/02/2000
 * Language:     C++
 *
 *
 *
 *****************************************************************************
 */

#include "common/ComSmallDefs.h"

#define COMMV_CTRL_PREFIX "@"

#define COMMV_IUD_LOG_SUFFIX   ""
#define COMMV_RANGE_LOG_SUFFIX ""
#define COMMV_CTX_LOG_SUFFIX   ""

#define COMMV_BEGINRANGE_PREFIX COMMV_CTRL_PREFIX "BR_"
#define COMMV_ENDRANGE_PREFIX   COMMV_CTRL_PREFIX "ER_"

#define COMMV_EPOCH_COL         COMMV_CTRL_PREFIX "EPOCH"
#define COMMV_CURRENT_EPOCH_COL COMMV_CTRL_PREFIX "CURRENT_EPOCH"
#define COMMV_OPTYPE_COL        COMMV_CTRL_PREFIX "OPERATION_TYPE"
#define COMMV_IGNORE_COL        COMMV_CTRL_PREFIX "IGNORE"
#define COMMV_BITMAP_COL        COMMV_CTRL_PREFIX "UPDATE_BITMAP"
#define COMMV_RANGE_SIZE_COL    COMMV_CTRL_PREFIX "RANGE_SIZE"
#define COMMV_ALIGNMENT_COL     COMMV_CTRL_PREFIX "ALIGNMENT"
#define COMMV_BASE_SYSKEY_COL   COMMV_CTRL_PREFIX "SYSKEY"
#define COMMV_RANGE_ID_COL      COMMV_CTRL_PREFIX "RANGE_ID"
#define COMMV_RANGE_TYPE_COL    COMMV_CTRL_PREFIX "RANGE_TYPE"

#endif  // COMMVDEFS_H
