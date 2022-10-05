
#ifndef CATLITERALS_H
#define CATLITERALS_H

/* -*-C++-*-
 *****************************************************************************
 *
 * File:         CatLiterals.h
 * Description:
 *
 *
 * Created:      11/18/96
 * Language:     C++
 *
 *
 *
 *
 *****************************************************************************
 */
#include "common/ComSizeDefs.h"

#define SYSTEM_CATALOG         "HP_SYSTEM_CATALOG"
#define SYSTEM_SCHEMA          "SYSTEM_SCHEMA"
#define SYSTEM_DEFAULTS_SCHEMA "SYSTEM_DEFAULTS_SCHEMA"
#define SYSTEM_DEFAULTS        "SYSTEM_DEFAULTS"
#define DEFINITION_SCHEMA      "HP_DEFINITION_SCHEMA"
#define SECURITY_SCHEMA        "HP_SECURITY_SCHEMA"

#define ROUTINES_SCHEMA    "HP_ROUTINES"
#define INFORMATION_SCHEMA "HP_INFORMATION_SCHEMA"

#define HISTOGRAMS           "HISTOGRAMS"
#define HISTOGRAM_INTERVALS  "HISTOGRAM_INTERVALS"
#define HISTOGRAMS_FREQ_VALS "HISTOGRAMS_FREQ_VALS"
#define PERSISTENT_SAMPLES   "PERSISTENT_SAMPLES"
#define PERSISTENT_DATA      "PERSISTENT_DATA"

// The max size of an ANSI identifier in internal format
#define MAX_ANSI_IDENTIFIER_SIZE_INTERNAL ComMAX_1_PART_INTERNAL_UTF8_NAME_LEN_IN_BYTES

// The max size of an ANSI identifier in external format
#define MAX_ANSI_IDENTIFIER_SIZE_EXTERNAL ComMAX_1_PART_EXTERNAL_UTF8_NAME_LEN_IN_BYTES

#endif  // CATLITERALS_H
