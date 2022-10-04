/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef HDFS_ORC_FILE_HH
#define HDFS_ORC_FILE_HH

#include <string>

#include "orc/orc-config.hh"
#include "orc/Reader.hh"
#include "exp/hdfs.h"
#include "orc/OrcFile.hh"

namespace orc {

  ORC_UNIQUE_PTR<InputStream> readHDFSFile(const std::string& path);

  ORC_UNIQUE_PTR<InputStream> readHDFSFile(hdfsFileInfo* fi, hdfsFS fs);

}

#endif
