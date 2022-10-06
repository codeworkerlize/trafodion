
#ifndef CONST_H
#define CONST_H

/* -*-C++-*-
******************************************************************************
*
* File:         Const.h
* RCS:          $Id: SConst.h,v 1.2 1997/04/23 00:29:08  Exp $
*
* Description:  This file contains the constant declarations common to all
*               classes related to ArkSort.
*
* Created:	05/20/96
* Modified:     $ $Date: 1997/04/23 00:29:08 $ (GMT)
* Language:     C++
* Status:       $State: Exp $
*
*
******************************************************************************
*/

// -----------------------------------------------------------------------
// Change history:
//
// $Log: SConst.h,v $
// Revision 1.2  1997/04/23 00:29:08
// Merge of MDAM/Costing changes into SDK thread
//
// Revision 1.1.1.2.2.1  1997/04/11 23:23:10
// Checking in partially resolved conflicts from merge with MDAM/Costing
// thread. Final fixes, if needed, will follow later.
//
// Revision 1.1.2.1  1997/04/10 18:31:02
// *** empty log message ***
//
// Revision 1.1.1.2  1997/04/02 06:25:23
// This is the latest from SourceSafe.
//
//
// 7     3/31/97 3:29p
// performance code merge
//
// 6     1/22/97 11:02p
// Merged UNIX and NT versions.
//
// 4     1/15/97 3:06a
// Put UNIX file on top.
//
// 2     1/13/97 12:42p
// milestone 4 snap
// Revision 1.3  1996/12/11 22:53:26
// Change is made in arksort to allocate memory from executor's space.
// Memory leaks existed in arksort code are also fixed.
//
// Revision 1.2  1996/11/27 00:14:31
// This checkin changes memcmp to str_cmp and also fixes the problem of missing
// rows in the result of order by queries.
//
// Revision 1.1  1996/08/15 14:47:36
// Initial revision
//
// Revision 1.1  1996/08/02 03:39:32
// Initial revision
//
// Revision 1.18  1996/05/20 16:32:34  <author_name>
// Added <description of the change>.
// -----------------------------------------------------------------------

#include "common/Platform.h"

#include "common/BaseTypes.h"
const int SORT_SUCCESS = 0;
const int SORT_FAILURE = 1;

const short REPL_SELECT = 1;
const short QUICKSORT = 2;
const short ITER_QUICKSORT = 3;
const int SCRATCH_BLOCK_SIZE = 56 * 1024;
const int MAXSCRFILES = 128;
const int FILENAMELEN = 48;
const int MAXRUNS = 512;
const int OVERHEAD = 20;  // The overhead for Scratch Buffer header struct

const short INITIAL_PHASE = 0;
const short RUN_GENERATION_PHASE = 1;
const short INTERMEDIATE_MERGE_PHASE = 2;
const short FINAL_MERGE_PHASE = 3;
const short EVERYTHING_DONE_PHASE = 4;

const short KEYS_ARE_EQUAL = 0;
const short KEY1_IS_SMALLER = -1;
const short KEY1_IS_GREATER = 1;

typedef int SBN;
const int TRUE_L = 1;
const int FALSE_L = 0;

#endif
