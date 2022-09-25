package org.apache.hadoop.hbase.pit.job;

import lombok.SneakyThrows;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.pit.ReplayEngine;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.TransactionMutationMsg;
import org.apache.hadoop.hbase.protobuf.ProtobufUtil;
import org.apache.hadoop.hbase.protobuf.generated.ClientProtos;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.lib.NullOutputFormat;
import org.apache.hadoop.util.*;
import org.apache.log4j.Logger;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author hjw
 * @date 20210812
 */
public class MutationReplayJob extends Configured implements Tool {
    private final static String CONF_TABLE_NAME_KEY = "mutation.replay.table.name";
    private final static String CONF_END_TIMESTAMP_KEY = "mutation.replay.end.timestamp";
    private final static String CONF_JARS_DEPENDENCY_KEY = "tmpjars";

    private final String tableName;
    private final long timestamp;
    private final List<String> mutationList;

    static Logger LOG = Logger.getLogger(MutationReplayJob.class);

    public MutationReplayJob(String tableName, long timestamp, List<String> mutationList) {
        this.tableName = tableName;
        this.timestamp = timestamp;
        this.mutationList = mutationList;
    }

    private static class ReplayMapper extends MapReduceBase implements Mapper<LongWritable, KeyValueWritable,
                NullWritable, NullWritable> {
        static Logger LOG = Logger.getLogger(MutationReplayJob.ReplayMapper.class);
        private FileSystem fs;
        private Table replayTable;
        private long replayTimestamp;

        @SneakyThrows
        @Override
        public void configure(JobConf job) {
            super.configure(job);
            fs = FileSystem.get(job);
            Connection connection = ConnectionFactory.createConnection(job);
            replayTable = connection.getTable(TableName.valueOf(job.get(CONF_TABLE_NAME_KEY)));
            replayTimestamp = job.getLong(CONF_END_TIMESTAMP_KEY, 0);
        }

        public void mutationReplay(List<Put> puts, List<Delete> deletes,  Table table) throws IOException {
            LOG.info("MutationReplay for table " + table.getName().getNameAsString()
                    + " #puts: " + puts.size()
                    + " #deletes: " + deletes.size()
            );
            try {
                table.put(puts);
                table.delete(deletes);
            } catch(IOException e) {
                LOG.error("MutationReplay Exception : ", e);
                throw e;
            }
        }

        @Override
        public void map(LongWritable key, KeyValueWritable value, OutputCollector<NullWritable, NullWritable> outputCollector, Reporter reporter) throws IOException {
            LOG.info("replay mapper start");
            ByteArrayInputStream input = new ByteArrayInputStream(CellUtil.cloneValue(value.get()));
            TransactionMutationMsg tmm  = TransactionMutationMsg.parseDelimitedFrom(input);
            List<Put> puts = Collections.synchronizedList(new ArrayList<Put>());
            List<Delete> deletes = Collections.synchronizedList(new ArrayList<Delete>());
            int mIndex = 0;
            int pIndex = 0;
            while (tmm != null) {
                long sid = tmm.getStartId();
                long cid = tmm.getCommitId();

                if (! replayTable.getName().getNameAsString().equals(tmm.getTableName())) {
                    LOG.error("PIT ZZZ error table name mismatch between replay engine and CDC record " +
                            replayTable.getName().getNameAsString() + " vs " + tmm.getTableName());
                }

                if ( cid <= replayTimestamp ) {
                    List<Boolean> putOrDel = tmm.getPutOrDelList();
                    List<ClientProtos.MutationProto> putProtos = tmm.getPutList();
                    List<ClientProtos.MutationProto> deleteProtos = tmm.getDeleteList();
                    int putIndex = 0;
                    int deleteIndex = 0;

                    int iBatch = 0;
                    for (Boolean put : putOrDel) {
                        pIndex++;
                        mIndex++;
                        if (put) {
                            Put writePut = ProtobufUtil.toPut(putProtos.get(putIndex++));
                            puts.add(writePut);
                        } else {
                            Delete writeDelete = ProtobufUtil.toDelete(deleteProtos.get(deleteIndex++));
                            deletes.add(writeDelete);
                        }
                        // TODO: 2021/8/17
                        if (mIndex >= 1000) {
                            mutationReplay(puts, deletes, replayTable);
                            iBatch++;
                            mIndex = 0;
                            puts.clear();
                            deletes.clear();
                            LOG.info("PIT mutationRead -- " +
                                    " replayTable: " + replayTable.getName().getNameAsString() +
                                    " replayTimestamp: " + replayTimestamp +
                                    " isPut: " + put +
                                    " Transaction Id " + tmm.getTxId() +
                                    " with startId " + sid +
                                    " and commitId " + cid +
                                    " is doing batch " + iBatch +
                                    " mutation " + pIndex);
                        }
                    }
                }
                tmm  = TransactionMutationMsg.parseDelimitedFrom(input);
            }

            if ((!puts.isEmpty()) || (!deletes.isEmpty())) {
                mutationReplay(puts, deletes, replayTable);
            }
            LOG.info("replay mapper end");
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        LOG.info("tableName is " + tableName + ",mutationList:" + mutationList + ",timestamp:" + timestamp);
        JobConf conf = new JobConf(getConf(), MutationReplayJob.class);
        conf.setJobName("MutationReplayJob");
        conf.setMapperClass(ReplayMapper.class);

        conf.setInputFormat(HFileInputFormatV2.class);
        conf.setOutputFormat(NullOutputFormat.class);

        conf.set(CONF_TABLE_NAME_KEY, tableName);
        conf.setLong(CONF_END_TIMESTAMP_KEY, timestamp);

        String dependencyList = DependencyCheckUtils.getDependencyHdfsPath(getConf());
        LOG.info("MutationReplayJob dependencyList:" + dependencyList);
        conf.set(CONF_JARS_DEPENDENCY_KEY, dependencyList);

        for (String mutationPath : mutationList) {
            FileInputFormat.addInputPath(conf,new Path(mutationPath));
        }
        Path outputPath = new Path("/tmp/br_restore", tableName.replace(":", "."));
        FileOutputFormat.setOutputPath(conf, new Path(outputPath, String.valueOf(System.currentTimeMillis())));

        JobClient.runJob(conf);
        return 0;
    }
}
