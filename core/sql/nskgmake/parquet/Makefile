#######################################################################
# @@@ START COPYRIGHT @@@
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#
# @@@ END COPYRIGHT @@@
#######################################################################

#INCLUDE_DIRS += $LIBHDFS_INC

# Define the correct compilation and linking flags depending on whether
# this is a debug or release build.
ifeq ($(FLAVOR),debug)
   # for coverage checking support
   ifeq ($(SQ_COVERAGE_OPTIMIZER),1)
      CXXFLAGS += --coverage
   endif
endif

#GUI Debugger
# Like in sqlcomp, enable this unconditionally, to be able to display in release mode
# if the necessary DLL is present
#CXXFLAGS += -DNA_DEBUG_GUI

#SYS_LIBS += 

SRCPATH := parquet parquet/util parquet/arrow parquet/arrow/io \
	   parquet/arrow/util parquet/arrow/compute \
	   parquet/arrow/compute/kernels

# first, files in parquet
CCSRC := column_scanner.cc \
	column_reader.cc \
	column_writer.cc \
	parquet_constants.cc \
	parquet_types.cc \
	exception.cc \
	file_reader.cc \
	file_writer.cc \
	metadata.cc \
	printer.cc \
	schema.cc \
	statistics.cc \
	types.cc \
	filter.cc  \
	page_filter.cc  \
	page_filter_lessthan.cc  \
	page_filter_greaterthan.cc  \
	value_filter.cc  \
	value_filter_greaterthan.cc  \
	value_filter_lessthan.cc  \
	value_filter_range.cc  \
	value_filter_eq.cc \
	value_filter_bf.cc  \
	value_filter_null.cc  
	
#value_filter.cc 

# next, files in parquet/util
CCSRC += comparison.cc  \
	memory.cc

# next, files in parquet/arrow
CCSRC += array.cc \
	arrow_schema.cc \
	buffer.cc \
	builder.cc \
	compare.cc  \
	memory_pool.cc \
	pretty_print.cc  \
	reader.cc \
	record_batch.cc  \
	record_reader.cc \
	status.cc \
	table.cc  \
	type.cc  \
	writer.cc

# next, files in parquet/arrow/io/
CCSRC += file.cc  \
	hdfs.cc \
	hdfs-internal.cc \
	interfaces.cc

# next, files in parquet/arrow/util
CCSRC += compression.cc \
	compression_snappy.cc \
	decimal.cc \
	io-util.cc  \
	tensor.cc  \
	bit-util.cc \
	key_value_metadata.cc \
	hash.cc \
	cpu-info.cc \
	ttime.cc

# next, files in parquet/arrow/compute
CCSRC += context.cc  \
	 cast.cc \
	 util-internal.cc


SYS_LIBS += -L$(THRIFT_LIB_DIR) -lthrift -L$(SNAPPY_LIB) -lsnappy $(LIBHDFS_LIB)



