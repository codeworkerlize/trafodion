
#ifndef EX_RCB_H
#define EX_RCB_H

/* -*-C++-*-
 *****************************************************************************
 *
 * File:         ex_rcb.h
 * Description:
 *
 *
 * Created:      7/10/95
 * Language:     C++
 *
 *
// ****************************************************************************
*/
//#include "comrcb.h"
#include "common/Collections.h"

// function to covert a MP record struct to ExRCB, called by filesystem.
// void * is returned so it does not need to include comrcb.h
void *ExRcbFromMpLabelInfo(void *recordPtr, void *sqlLabel, short entry_seq, CollHeap *heap);

short getExRcbSize();

#endif
