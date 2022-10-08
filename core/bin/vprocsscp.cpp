
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         vprocsscp.cpp
 * Created:      2006-05-19
 * Language:     C++
 *
 *
 *****************************************************************************
 */

#include "bin/vproc.h"

extern "C" {
void VPROC(PRODNUMSSCP, DATE1SSCP, SSCP_CC_LABEL)() {}
}

#include "common/SCMVersHelp.h"

#define VERS_CV_MAJ 1
#define VERS_CV_MIN 0
#define VERS_CV_UPD 1

VERS_BIN(mxsscp)
