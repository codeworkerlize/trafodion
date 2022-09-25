/* 
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.parquet.hadoop;

import static org.apache.parquet.Preconditions.checkNotNull;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.Logger;

import org.apache.parquet.Preconditions;
import org.apache.parquet.filter.UnboundRecordFilter;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.FilterCompat.Filter;
import org.apache.parquet.hadoop.Footer;
import org.apache.parquet.hadoop.InternalParquetRecordReader;
import org.apache.parquet.hadoop.api.ReadSupport;
import org.apache.parquet.hadoop.api.ReadSupport.ReadContext;

import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.FileMetaData;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;

import org.apache.parquet.hadoop.util.HiddenFileFilter;

import org.trafodion.sql.TrafParquetFileReader.timer;

/**
 * Read records from a Parquet file.
 * TODO: too many constructors (https://issues.apache.org/jira/browse/PARQUET-39)
 */
public class TrafParquetReader<T> implements Closeable {

  static Logger logger = Logger.getLogger(TrafParquetReader.class.getName());;

  private final ReadSupport<T> readSupport;
  private final Configuration conf;
  private final Iterator<Footer> footersIterator;
  private final Filter filter;
  private long offset;
  private long length;
  private ReadContext    readContext;
  private timer          m_timer = null;
  private boolean        m_first_call_after_init = true;

  private InternalParquetRecordReader<T> reader;

  /**
   * @param file the file to read
   * @param readSupport to materialize records
   * @throws IOException
   * @deprecated use {@link #builder(ReadSupport, Path)}
   */
  @Deprecated
  public TrafParquetReader(Path file, ReadSupport<T> readSupport) throws IOException {
    this(new Configuration(), file, readSupport, FilterCompat.NOOP);
  }

  /**
   * @param conf the configuration
   * @param file the file to read
   * @param readSupport to materialize records
   * @throws IOException
   * @deprecated use {@link #builder(ReadSupport, Path)}
   */
  @Deprecated
  public TrafParquetReader(Configuration conf, Path file, ReadSupport<T> readSupport) throws IOException {
    this(conf, file, readSupport, FilterCompat.NOOP);
  }

  /**
   * @param file the file to read
   * @param readSupport to materialize records
   * @param unboundRecordFilter the filter to use to filter records
   * @throws IOException
   * @deprecated use {@link #builder(ReadSupport, Path)}
   */
  @Deprecated
  public TrafParquetReader(Path file, ReadSupport<T> readSupport, UnboundRecordFilter unboundRecordFilter) throws IOException {
    this(new Configuration(), file, readSupport, FilterCompat.get(unboundRecordFilter));
  }

  /**
   * @param conf the configuration
   * @param file the file to read
   * @param readSupport to materialize records
   * @param unboundRecordFilter the filter to use to filter records
   * @throws IOException
   * @deprecated use {@link #builder(ReadSupport, Path)}
   */
  @Deprecated
  public TrafParquetReader(Configuration conf, Path file, ReadSupport<T> readSupport, UnboundRecordFilter unboundRecordFilter) throws IOException {
    this(conf, file, readSupport, FilterCompat.get(unboundRecordFilter));
  }

  private TrafParquetReader(Configuration conf,
			    Path file,
			    ReadSupport<T> readSupport,
			    Filter filter) throws IOException {
      if (logger.isTraceEnabled()) logger.trace("TrafParquetReader" 
						+ ", file: " + file
						+ ", readSupport: " + readSupport
						);
    this.readSupport = readSupport;
    this.filter = checkNotNull(filter, "filter");
    this.conf = conf;
    this.offset = -1;
    this.length = 0;

    FileSystem fs = file.getFileSystem(conf);
    List<FileStatus> statuses = Arrays.asList(fs.listStatus(file, HiddenFileFilter.INSTANCE));
    if (logger.isTraceEnabled()) logger.trace("TrafParquetReader" 
					      + ", statuses.size: " + statuses.size()
					      );

    List<Footer> footers = ParquetFileReader.readAllFootersInParallelUsingSummaryFiles(conf, statuses, false);

    this.footersIterator = footers.iterator();
  }

  /**
   * @return the next record or null if finished
   * @throws IOException
   */
  public T read() throws IOException {
    try {
      if (reader != null && reader.nextKeyValue()) {
	  T l_val = reader.getCurrentValue();
	  if (m_first_call_after_init) {
	      if (m_timer != null) {
		  m_timer.stop2();
	      }
	      m_first_call_after_init = false;
	  }
	  return l_val;
      } else {
	  if (m_timer != null) {
	      m_timer.start2();
	      m_first_call_after_init = true;
	  }
        initReader();
	if (reader == null) {
	    if (m_timer != null) {
		m_timer.stop2();
	    }
	    m_first_call_after_init = false;
	}
        return reader == null ? null: read();
      }
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  /**
   * @return the next record or null if finished
   * @throws IOException
   */
  public long read2() throws IOException {

      long lv_count = 0;
      boolean lv_done = false;

      try {

	do {
	    if (m_timer != null) {
		m_timer.start2();
	    }

	    initReader();
	    
	    if (reader == null) {
		lv_done = true;
		break;
	    }
	    
	    while (reader.nextKeyValue()) {
		T l_val = reader.getCurrentValue();
		++lv_count;
	    }

	    if (m_timer != null) {
		m_timer.stop2();
	    }
	}
	while ( ! lv_done);
	
      } catch (InterruptedException e) {
	  throw new IOException(e);
      }
      
      return lv_count;
  }

  private void initReader() throws IOException {
    if (reader != null) {
      reader.close();
      reader = null;
    }
    if (footersIterator.hasNext()) {
      Footer footer = footersIterator.next();

      ParquetMetadata lv_pmd_file = footer.getParquetMetadata();

      List<BlockMetaData> lv_all_blocks_in_file = footer.getParquetMetadata().getBlocks();
      if (logger.isTraceEnabled()) logger.trace("TrafParquetReader" 
						+ ", #blocks in file: " + 
						(lv_all_blocks_in_file == null ? "null" : lv_all_blocks_in_file.size())
						);

      ParquetMetadata lv_pmd = null;
      if (offset == -1) {
	  lv_pmd = lv_pmd_file;
      }
      else {
	  List<BlockMetaData> lv_blocks_to_read = new ArrayList<BlockMetaData>();

	  long lv_curr_offset = offset;
	  long lv_max_offset = offset + length;
	  long lv_total_data_to_read = length;
	  long lv_total_data_already_read = 0;
	  for (BlockMetaData lv_block : lv_all_blocks_in_file) {
	      long lv_starting_pos = lv_block.getStartingPos();
	      long lv_block_size = lv_block.getTotalByteSize();
	      if (logger.isTraceEnabled()) logger.trace("TrafParquetReader" 
							+ ", file:" + footer.getFile()
							+ ", starting_pos: " + lv_starting_pos
							+ ", block_size: " + lv_block_size
							+ ", curr_offset: " + lv_curr_offset
							+ ", data_already_read: " + lv_total_data_already_read
							+ ", max_offset: " + lv_max_offset
							);
	      if (lv_curr_offset <= lv_starting_pos) {
		  lv_blocks_to_read.add(lv_block);
		  lv_curr_offset = lv_starting_pos + lv_block_size;
		  lv_total_data_already_read += lv_block_size;
	      }
	      // if (lv_curr_offset >= lv_max_offset) {
	      if (lv_total_data_already_read >= lv_total_data_to_read) {
		  break;
	      }
	  }
	  lv_pmd = new ParquetMetadata(lv_pmd_file.getFileMetaData(), lv_blocks_to_read);
      }

      if (logger.isTraceEnabled()) logger.trace("TrafParquetReader"
						+ ", file:" + footer.getFile()
						+ ", #blocks to read: "
						+ (lv_pmd==null ? "null" : lv_pmd.getBlocks().size())
						);
      
      ParquetFileReader fileReader = ParquetFileReader.open(
          conf, footer.getFile(), lv_pmd);
      long lv_before_filter_block_count = fileReader.getRowGroups().size();

      // apply data filters
      fileReader.filterRowGroups(filter);
      long lv_after_filter_block_count = fileReader.getRowGroups().size();

      if (logger.isTraceEnabled()) logger.trace("before InternalParquetRecordReader "
						+ ", readContext: " 
						+ (readContext==null?"null":readContext.getRequestedSchema())
						);

      if (logger.isDebugEnabled()) logger.debug("file: " + footer.getFile()
						+ ", offset: " + offset
						+ ", length: " + length
						+ ", filter: " + filter
						+ ", #blk_before_filter: " + lv_before_filter_block_count
						+ ", #blk_after_filter: " + lv_after_filter_block_count
						);

      reader = new InternalParquetRecordReader<T>(readSupport, filter);

      reader.initialize(fileReader, conf, readContext);
    }

  }

  @Override
  public void close() throws IOException {
    if (reader != null) {
      if (logger.isTraceEnabled()) logger.trace("TrafParquetReader"
						+ ", close reader: " + reader
						);
      reader.close();
    }
  }

  public static <T> Builder<T> builder(ReadSupport<T> readSupport, Path path) {
    return new Builder<T>(readSupport, path);
  }

  private void setBlockOffsetInfo(long pv_offset, long pv_length) {
      this.offset = pv_offset;
      this.length = pv_length;
  }

  private void setReadContext(ReadContext pv_rc) {
      this.readContext = pv_rc;
  }

  private void setTimer(timer pv_timer) {
      this.m_timer = pv_timer;
  }

  public static class Builder<T> {
    private final ReadSupport<T> readSupport;
    private final Path file;
    private ReadContext    readContext;
    private Filter filter;
    protected Configuration conf;
    private long offset;
    private long length;
    private timer m_timer = null;

    private Builder(ReadSupport<T> readSupport, Path path) {
      this.readSupport = checkNotNull(readSupport, "readSupport");
      this.file = checkNotNull(path, "path");
      this.conf = new Configuration();
      this.filter = FilterCompat.NOOP;
      this.offset = -1;
      this.length = 0;
    }

    protected Builder(Path path) {
      this.readSupport = null;
      this.file = checkNotNull(path, "path");
      this.conf = new Configuration();
      this.filter = FilterCompat.NOOP;
    }

    public Builder<T> withConf(Configuration conf) {
      this.conf = checkNotNull(conf, "conf");
      return this;
    }

    public Builder<T> withFilter(Filter filter) {
      this.filter = checkNotNull(filter, "filter");
      return this;
    }

    public Builder<T> withReadContext(ReadContext pv_rc) {
      this.readContext = pv_rc;
      return this;
    }

    /*
     * pv_offset starting offset of the block
     *           specifying -1 (the default) means all the blocks in the file
     * pv_length #bytes from pv_offset
     *           - pv_offset + pv_length could span multiple blocks
     *           - pv_offset + pv_length should completely cover the last block
     */
    public Builder<T> withBlockOffsetInfo(long pv_offset, long pv_length) {
      this.offset = pv_offset;
      this.length = pv_length;
      return this;
    }

    public Builder<T> withTimer(timer pv_timer) {
      this.m_timer = pv_timer;
      return this;
    }

    protected ReadSupport<T> getReadSupport() {
      // if readSupport is null, the protected constructor must have been used
      Preconditions.checkArgument(readSupport != null,
          "[BUG] Classes that extend Builder should override getReadSupport()");
      return readSupport;
    }

    public ReadContext getReadContext() {
      return readContext;
    }

    public TrafParquetReader<T> build() throws IOException {
      TrafParquetReader lv_tpr = new TrafParquetReader<T>(conf, file, getReadSupport(), filter);
      lv_tpr.setBlockOffsetInfo(offset, length);
      lv_tpr.setReadContext(readContext);
      lv_tpr.setTimer(m_timer);
      return lv_tpr;
    }
  }
}
