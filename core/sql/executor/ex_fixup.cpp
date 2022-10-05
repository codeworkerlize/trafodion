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
// @@@ START COPYRIGHT @@@
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// @@@ END COPYRIGHT @@@
 *
 *
 *****************************************************************************
 */

#include "common/ExCollections.h"
#include "exp/exp_stdh.h"
#include "executor/ex_stdh.h"
#include "comexe/ComTdb.h"
#include "executor/ex_tcb.h"
#include "ex_root.h"
#include "ex_onlj.h"
#include "ex_update.h"
#include "ex_delete.h"
#include "ex_union.h"
#include "ex_tuple.h"
#include "ex_hash_grby.h"
#include "ex_sort_grby.h"
#include "ex_split_top.h"
#include "ex_split_bottom.h"
#include "ex_send_top.h"
#include "ex_send_bottom.h"
#include "ex_part_input.h"
#include "ex_hashj.h"
#include "ex_mj.h"
#include "ex_dp2exe_root.h"
#include "ex_partn_access.h"

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
