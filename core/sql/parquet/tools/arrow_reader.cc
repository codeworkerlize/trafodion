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

#include <fstream>
#include <list>
#include <memory>
//#include <sys/times.h>
#include <sys/time.h>
#include "arrow/reader.h"
#include "arrow/table.h"
#include "arrow/io/arrow_hdfs.h"

//#undef ACCESS_HDFS
//#define ACCESS_HDFS 1

//#ifdef ACCESS_HDFS
#include <hdfs.h>
#include "arrow/io/arrow_hdfs.h"
//#else
#include "arrow/io/file.h"
//#endif


#define ONE_MB (1024 * 1024)

using namespace std;

void doRead(std::unique_ptr<parquet::arrow::FileReader> &reader,
                         const std::vector<int>& column_subset,
                         std::shared_ptr<arrow::Table>* out) 
{
  ::arrow::Status s;

  if (column_subset.size() > 0) {
    s = reader->ReadTable(column_subset, out);
  } else {
    // Read everything
    s = reader->ReadTable(out);
  }

  if (!s.ok()) {
    cout << "ReadTable() failed." << endl; 
  }
}

void readHDFSDataFile(std::string& path, int bufSize, int num_threads,
                         const std::vector<int>& column_subset,
                         std::shared_ptr<arrow::Table>* out) 
{
  std::unique_ptr<parquet::arrow::FileReader> reader;

  std::shared_ptr<arrow::io::HadoopFileSystem> hdfsClient;

  // to do:
  // swhdfs getconf -confKey fs.defaultFS
  //  hdfs://localhost:30000
  arrow::io::HdfsConnectionConfig conf;

  char* host = getenv("HOST");
  conf.host = (host) ? host : "localhost";

  char* user = getenv("USER");
  conf.user = (user) ? user : "trafodion";

  char* port = getenv("PORT");
  conf.port = (port) ? atoi(port) : 8020;

  arrow::io::HadoopFileSystem::Connect(&conf, &hdfsClient);

  std::shared_ptr<arrow::io::HdfsReadableFile> hdfsFile;
  ::arrow::Status s = hdfsClient->OpenReadable(path, bufSize, &hdfsFile);

  if (!s.ok()) {
    cout << "OpenReadable() failed." << endl; 
  }

  s = (OpenFile(
                hdfsFile,
                ::arrow::default_memory_pool(),
                ::parquet::default_reader_properties(), 
                nullptr, // metadata
                &reader));

  if (!s.ok()) {
    cout << "OpenFil() failed." << endl; 
  }

  reader->set_num_threads(num_threads);

  doRead(reader, column_subset, out);
}

void readDataFile(std::string& path, int bufSize, int num_threads,
                  const std::vector<int>& column_subset,
                  std::shared_ptr<arrow::Table>* out) 
{
  std::unique_ptr<parquet::arrow::FileReader> reader;

  std::shared_ptr<::arrow::io::ReadableFile> file;
  ::arrow::Status s = arrow::io::ReadableFile::Open(path, &file);

  if (!s.ok()) {
    cout << "OpenReadable() failed." << endl; 
  }

  s = (OpenFile(
                file,
                ::arrow::default_memory_pool(),
                ::parquet::default_reader_properties(), 
                nullptr, // metadata
                &reader));

  if (!s.ok()) {
    cout << "OpenFil() failed." << endl; 
  }

  reader->set_num_threads(num_threads);

  doRead(reader, column_subset, out);
}

int main(int argc, char** argv) {
  if (argc > 5 || argc < 2) {
    std::cerr << "Usage: arrow_reader [--hdfs] [--only-metadata] [--num_threads]"
                 "[--columns=...] <file>"
              << std::endl;
    return -1;
  }

  std::string filename;
  bool print_values = true;
  bool read_hdfs = false;

  // Read command-line options
  const std::string COLUMNS_PREFIX = "--columns=";
  std::vector<int> columns;

  int num_threads = 1;

  char *param, *value;
  for (int i = 1; i < argc; i++) {
    if ((param = std::strstr(argv[i], "--hdfs"))) {
      read_hdfs = true;
    } else
    if ((param = std::strstr(argv[i], "--only-metadata"))) {
      print_values = false;
    } else
    if ((param = std::strstr(argv[i], "--num_threads"))) {
      if ( i < argc - 1 ) {
        num_threads = atoi(argv[i+1]);
        i++;
      }
    } else if ((param = std::strstr(argv[i], COLUMNS_PREFIX.c_str()))) {
      value = std::strtok(param + COLUMNS_PREFIX.length(), ",");
      while (value) {
        columns.push_back(std::atoi(value));
        value = std::strtok(nullptr, ",");
      }
    } else {
      filename = argv[i];
    }
  }

  int bufSize = 512 * ONE_MB;

  try {

      struct timeval start, end;

      //tms startTms;
      //clock_t startClock = times(&startTms);
      gettimeofday(&start, NULL);     


      std::shared_ptr<arrow::Table> result;

      if ( read_hdfs )
         readHDFSDataFile(filename, bufSize, num_threads, columns, &result);
      else
         readDataFile(filename, bufSize, num_threads, columns, &result);

      //result->display(std::cout);
      result->scan(std::cout);

      //tms endTms;
      //clock_t endClock = times(&startTms);
      gettimeofday(&end, NULL);     


      int64_t start_us = (start.tv_sec * (int64_t)1000000) + start.tv_usec;
      int64_t end_us = (end.tv_sec * (int64_t)1000000) + end.tv_usec;

      cout << "File to scan:" << filename.c_str();
      cout << ", # of threads:" <<  num_threads;
      cout << ", # of rows read:" << result->num_rows();
      cout << ", ET(s):" << float(end_us-start_us) / 1000000 << endl;


  } catch (const std::exception& e) {
    std::cerr << "Parquet error: " << e.what() << std::endl;
    return -1;
  }

  return 0;
}
