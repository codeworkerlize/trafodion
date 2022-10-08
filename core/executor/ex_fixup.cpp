/* -*-C++-*-
 *****************************************************************************
 *
 * File:         <file>
 * Description:
 *
 *
 * Created:      7/10/95
 * Language:     C++
 *
 *

 *
 *
 *****************************************************************************
 */

#include "comexe/ComTdb.h"
#include "common/ExCollections.h"
#include "executor/ex_hash_grby.h"
#include "executor/ex_hashj.h"
#include "executor/ex_mj.h"
#include "executor/ex_onlj.h"
#include "executor/ex_root.h"
#include "executor/ex_send_bottom.h"
#include "executor/ex_send_top.h"
#include "executor/ex_sort_grby.h"
#include "executor/ex_split_bottom.h"
#include "executor/ex_split_top.h"
#include "executor/ex_tuple.h"
#include "executor/ex_union.h"

#include "executor/ex_stdh.h"
#include "executor/ex_tcb.h"
#include "exp/exp_stdh.h"

int ex_tdb::fixup(int /*base*/) { return 0; };

int ex_root_tdb::fixup(int /*base*/) { return 0; };

int ex_onlj_tdb::fixup(int /*base*/) { return 0; }

int ex_hashj_tdb::fixup(int /*base*/) { return 0; }

int ex_mj_tdb::fixup(int /*base*/) { return 0; }

int ex_update_tdb::fixup(int /*base*/) { return 0; }

int ex_delete_tdb::fixup(int /*base*/) { return 0; }

int ex_union_tdb::fixup(int /*base*/) { return 0; }

int ex_tuple_tdb::fixup(int /*base*/) { return 0; }

int ex_hash_grby_tdb::fixup(int /*base*/) { return 0; }

int ex_sort_grby_tdb::fixup(int /*base*/) { return 0; }

int ex_split_top_tdb::fixup(int base) {
  if (partInputDataDesc_) return partInputDataDesc_->fixup(base);
  return 0;
}

int ex_split_bottom_tdb::fixup(int /*base*/) { return 0; }

int ex_send_top_tdb::fixup(int /*base*/) { return 0; }

int ex_send_bottom_tdb::fixup(int /*base*/) { return 0; }

int ex_dp2exe_root_tdb::fixup(int /*base*/) { return 0; }

int ex_partn_access_tdb::fixup(int /*base*/) { return 0; }

int ExPartInputDataDesc::fixup(int /*base*/) { return 0; }

int ExHashPartInputData::fixup(int /*base*/) { return 0; }

int ExRoundRobinPartInputData::fixup(int /*base*/) { return 0; }

int ExRangePartInputData::fixup(int /*base*/) { return 0; }
