
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         vprocutp.cpp
 * Created:      06/22/2010
 * Language:     C++
 *
 *
//=============================================================================
//
*
*****************************************************************************
*/

#include <stdio.h>

#include "vproc.h"

extern "C" {
void VPROC(PRODNUMMXUDR, DATE1MXUDR, MXUDR_CC_LABEL)() {}
}

#include "common/SCMVersHelp.h"

#define VERS_CV_MAJ 1
#define VERS_CV_MIN 0
#define VERS_CV_UPD 1

VERS_BIN(tdm_udrserv)
