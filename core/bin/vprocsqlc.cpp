
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         vprocsqlc.CPP
 * Created:      08/03/00
 * Language:     C++
 *
 *
//=============================================================================
//
*
*****************************************************************************
*/

#include <stdio.h>

#include "bin/vproc.h"

extern "C" {
void VPROC(PRODNUMMXSQLC, DATE1MXSQLC, MXSQLC_CC_LABEL)() {}
}

#include "common/SCMVersHelp.h"

#define VERS_CV_MAJ 1
#define VERS_CV_MIN 0
#define VERS_CV_UPD 1

VERS_BIN(sqlc)
