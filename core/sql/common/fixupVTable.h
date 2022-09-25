/**********************************************************************
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
**********************************************************************/
#ifndef FIXUP_VTABLE_H
#define PFIXUP_VTABLE_H

class QualifiedName;
class NAString;
class HTableCache;
class HTableRow;

// fixupVTable() copies the virtual table pointer (1st 8 bytes)
// in a C++ onject , from a valid source object to target object.
// The target object can be on shared cache where its vurtual
// table object pointer is created by another process and therefore
// invalid in the current process.
//
// General versions that do nothing.
void fixupVTable(void*);
void fixupVTable(const void*);

// Special versions created for table descriptor shared cache 
// that does the pointer copy.
void fixupVTable(QualifiedName*);
void fixupVTable(NAString*);
void fixupVTable(HTableCache*);
void fixupVTable(HTableRow*);

#endif
