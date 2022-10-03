

#ifndef COMTDBCOMMON_H
#define COMTDBCOMMON_H

// --------------------------------------------------------------------------
// This header file includes the common header files needed by the generator
// implementations of all TDB subclasses.
// --------------------------------------------------------------------------

// These headers are needed in all implementations of TDB::pack().
#include "exp/exp_stdh.h"        // rewrite PACK_CRI_DESC() to ex_cri_desc->pack()
#include "exp/ExpCriDesc.h"      // for ex_cri_desc->pack()

#endif
