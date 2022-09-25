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
/* -*-C++-*-
 *****************************************************************************
 *
 * File:         filter.cc
 *
 * Description:  All filters class related code.
 *
 * Created:      9/2/2018
 * Language:     C++
 *
 *
 *****************************************************************************
 */
#include <iostream>
#include "filter.h"
#include "page_filter.h"
#include "value_filter.h"

Filters::~Filters()
{
  PageFilter* p = getPageFilter();

  while (p) {
    PageFilter* q = p;
    p = dynamic_cast<PageFilter*>(p->getNext());
    delete q;
  }

  ValueFilter* v = getValueFilter();
  while (v) {
    ValueFilter* w = v;
    v = dynamic_cast<ValueFilter*>(v->getNext());
    delete w;
  }
}

void Filters::resetCounters()
{
  ParquetFilter* p = getPageFilter();

  while (p) {
    p->resetCounter();
    p=p->getNext();
  }

  p = getValueFilter();
  while (p) {
    p->resetCounter();
    p=p->getNext();
  }
}

/*
void Filters::disableBloomFilters()
{
  ParquetFilter* p = getValueFilter();
  while (p) {
    p->disableBloomFilter();
    p=p->getNext();
  }
}
*/

void Filters::display(const char* msg)
{
  std::cout << msg << std::endl;

  PageFilter* p = getPageFilter();

  std::cout << "Page filters:" << std::endl;
  while (p) {
    p->display(); std::cout << std::endl;
    p = dynamic_cast<PageFilter*>(p->getNext());
  }

  std::cout << "Value filters:" << std::endl;
  ValueFilter* v = getValueFilter();
  while (v) {
    v->display(); std::cout << std::endl;
//std::cout << "next=" << static_cast<void*>(v->getNext());
    v = dynamic_cast<ValueFilter*>(v->getNext());
  }

  std::cout << std::endl;
}

