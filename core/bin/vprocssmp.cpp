
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         vprocssmp.cpp
 * Created:      2006-05-19
 * Language:     C++
 *
 *
 *****************************************************************************
 */

#include "bin/vproc.h"

extern "C" {
void VPROC(PRODNUMSSMP, DATE1SSMP, SSMP_CC_LABEL)() {}
}

#include "common/SCMVersHelp.h"

#define VERS_CV_MAJ 1
#define VERS_CV_MIN 0
#define VERS_CV_UPD 1

VERS_BIN(mxssmp)
