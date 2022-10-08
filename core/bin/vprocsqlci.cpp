
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         vprocsqlci.CPP
 * Created:      02/26/99
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
void VPROC(PRODNUMMXCI, DATE1MXCI, MXCI_CC_LABEL)() {}
}

#include "common/SCMVersHelp.h"

#define VERS_CV_MAJ 1
#define VERS_CV_MIN 0
#define VERS_CV_UPD 1

VERS_BIN(sqlci)
