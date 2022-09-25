/* -*-C++-*-
****************************************************************************
*
* File:         NAExecTrans.cpp
* Description:  
*
* Created:      5/6/98
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
****************************************************************************
*/

#include "NAExecTrans.h"
#include "sqlcli.h"
#include "sql_id.h"

//   return TRUE, if there is a transaction, transId will hold 
//                transaction ID if passed in.
//   return FALSE, if there is no transaction
NABoolean NAExecTrans(Int64 *transId)
{
  short retcode = 0;

  Int64 l_transid;
  retcode = GETTRANSID((short *)&l_transid);
  if (transId)
    *transId = l_transid;

  return (retcode == 0);
}


