package org.apache.hadoop.hbase.pit.job;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapred.*;
import org.apache.log4j.Logger;
import java.io.IOException;

/**
 * @author hjw
 * @date 20210812
 */
public class HFileInputFormatV2 extends FileInputFormat implements JobConfigurable {
    private static final Logger LOG = Logger.getLogger(HFileInputFormatV2.class);
    @Override
    public void configure(JobConf jobConf) {

    }

    @Override
    protected boolean isSplitable(FileSystem fs, Path file) {
        return false;
    }

    private class HFileRecordReadV2 implements RecordReader<LongWritable, KeyValueWritable> {

        private final HFile.Reader reader;
        private final HFileScanner scanner;
        private boolean hasNext;
        private final long start;
        private long pos;
        private final long end;

        public HFileRecordReadV2(FileSplit split, Configuration conf) throws IOException {
            final Path path = split.getPath();
            reader = HFile.createReader(FileSystem.get(conf), path, new CacheConfig(conf),conf);
            scanner = reader.getScanner(true, false);
            this.start = split.getStart();
            this.end = this.start + split.getLength();
            this.pos = this.start;
            this.hasNext = scanner.seekTo();
            LOG.info("create HFileRecordReadV2 instance,path:" + path + ",start:" + this.start + ",split.getLength:" + split.getLength());
        }

        @Override
        public boolean next(LongWritable key, KeyValueWritable value) throws IOException {
            if (this.hasNext) {
                this.pos++;
                key.set(this.pos);
                value.set(scanner.getKeyValue());
                this.hasNext = scanner.next();
                return true;
            }
            LOG.info("read mutation file end, pos:" + this.pos);
            return false;
        }

        @Override
        public LongWritable createKey() {
            return new LongWritable();
        }

        @Override
        public KeyValueWritable createValue() {
            return new KeyValueWritable();
        }

        @Override
        public long getPos() throws IOException {
            return this.pos;
        }

        @Override
        public void close() throws IOException {
            if (reader != null) {
                reader.close();
            }
        }

        @Override
        public float getProgress() throws IOException {
            if (this.start == this.end) {
                return 0.0F;
            } else {
                return Math.min(1.0F, (float)(this.pos - this.start) / (float)(this.end - this.start));
            }
        }
    }

    @Override
    public RecordReader<LongWritable, KeyValueWritable> getRecordReader(InputSplit genericSplit, JobConf job, Reporter reporter) throws IOException {
        reporter.setStatus(genericSplit.toString());
        return new HFileRecordReadV2((FileSplit)genericSplit, job);
    }
}
