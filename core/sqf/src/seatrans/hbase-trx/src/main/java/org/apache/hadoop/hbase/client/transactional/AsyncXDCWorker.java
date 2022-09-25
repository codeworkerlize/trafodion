package org.apache.hadoop.hbase.client.transactional;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.coprocessor.transactional.generated.TrxRegionProtos.TransactionMutationMsg;
import org.apache.hadoop.hbase.pit.HBaseBinlog;
import org.apache.hadoop.hbase.pit.HBaseBinlog.RowKey;
import org.apache.log4j.PropertyConfigurator;


public class AsyncXDCWorker {

   private static final int  BUFFER_QUEUE_SIZE      = 1000;

   static final Log LOG = LogFactory.getLog(AsyncXDCWorker.class);

   public static void setupConf() {
      String confFile = System.getProperty("trafodion.log4j.configFile");
      if (confFile == null) {
         System.setProperty("hostName", System.getenv("HOSTNAME"));
         System.setProperty("trafodion.atrxdc.log", System.getenv("TRAF_LOG") + "/trafodion.atrxdc.java.${hostName}.log");
         confFile = System.getenv("TRAF_CONF") + "/log4j.atrxdc.config";
      }
      PropertyConfigurator.configure(confFile);
   }

   public static void main(final String[] args)
   {
      setupConf();
      LOG.info("AsyncXDCWorker worker on salt: " + args[0] + ", partialNum: " + args[1]);
      try {
         int binlogPartNum = Integer.parseInt(args[1]);
         CopyContext context = new CopyContext(Integer.parseInt(args[0]), binlogPartNum);
         try (Connection local_conn = ConnectionFactory.createConnection(context.local_conf);
              Table local_binlog_table = local_conn.getTable(HBaseBinlog.getTableName())) {

            LOG.info("AsynXDCWorker work on binlog salt: " + context.my_salt);
            ContextDependentTask.TaskDependentContext depContext = new ContextDependentTask.TaskDependentContext();
            if (context.my_salt < 0) {
               LOG.warn("AsyncXDCWorker salt less than 0, fetch all rowkey and put to stdout then exit");
               BinlogFetcher.fetchAndPrintAll(context.master_conf);
               return;
            }

            HBaseBinlog.RowKey currentHighestRowKey = HBaseBinlog.BinlogStats.getBinlogStats(local_binlog_table, context.my_salt, binlogPartNum).highest_rowkey;
            new ContextDependentTask(depContext, new BinlogFetcher(context.master_conf, currentHighestRowKey, context.partialNum, context.buffer));
            // TODO: consider multi writer
            new ContextDependentTask(depContext,new BinlogWriter(context.local_conf, context.buffer));
            final ExecutorService pool = Executors.newFixedThreadPool(depContext.dependentTasks.size());
            for (Runnable task : depContext.dependentTasks) {
               pool.submit(task);
            }
            depContext.waitContext();
            pool.shutdown();
         }
      } catch (final Exception e) {
         LOG.error("AsyncXDCWorker exit with error: ", e);
      }
   }

   public static interface FailableTask {
      public void run() throws Exception;
      public void stop();
   }

   public static class ContextDependentTask implements Runnable {
      TaskDependentContext context;
      FailableTask task;

      public static class TaskDependentContext {
         public Exception exception = null;
         public boolean running = true;
         public List<ContextDependentTask> dependentTasks;

         public TaskDependentContext() {
            this.dependentTasks = new ArrayList<>();
         }

         public void addTask(ContextDependentTask task) {
            dependentTasks.add(task);
         }

         public void waitContext() throws InterruptedException {
            synchronized (this) {
               while (running) {
                  this.wait();
               }
               if (exception != null)
                  LOG.error("TaskDependentContext shutting down due to exception: ", exception);
               for (ContextDependentTask task : dependentTasks) {
                  task.stop();
               }
            }
         }
      }

      public ContextDependentTask(TaskDependentContext context, FailableTask task) {
         this.context = context;
         this.task = task;
         this.context.addTask(this);
      }

      @Override
      public void run() {
		  if (!Thread.currentThread().getName().startsWith("C")) {
			  Thread.currentThread().setName("ContextDependentTask-" + Thread.currentThread().getName());
		  }

         try {
            task.run();
         }
         catch (Exception e) {
            LOG.error("ContextDependentTask except:", e);
            synchronized (context) {
               context.exception = e;
            }
         }
         synchronized (context) {
            context.running = false;
            context.notifyAll();
         }
      }

      public void stop() {
         task.stop();
      }
   }

   public static class CopyContext {
      public int                 my_salt;
      public int                 partialNum;
      public Configuration       local_conf;
      public Configuration       master_conf;
      public STRConfig           str_conf;
      public BlockingQueue<Cell> buffer;

      static final Log LOG = LogFactory.getLog(CopyContext.class);

      public CopyContext(final int salt, final int partionNum) throws ZooKeeperConnectionException, IOException {
         this.my_salt = salt;
         this.partialNum = partionNum;
         LOG.debug("creating local configuration");
         local_conf = HBaseConfiguration.create();

         LOG.debug("getting STRConfig instance");
         str_conf = STRConfig.getInstance(local_conf);

         LOG.debug("getting master configuration");
         master_conf = str_conf.getPeerConfiguration(str_conf.getFirstRemotePeerId());
         if (master_conf == null) {
            LOG.error("AsyncXDCWorker CopyContext can't get master hbase configuration, please check you XDC config");
            throw new IOException("Master hbase configuration is null");
         }
         buffer = new LinkedBlockingDeque<>(BUFFER_QUEUE_SIZE);

         LOG.info("CopyContext initialized");
      }
   }

   public static class BinlogFetcher implements FailableTask {
      public static final int WAIT_MASTER_MS         = 1000;
      public static final int MAX_ERROR_COUNTER      = 1000;

      private static final long BIG_WID_JUMP         = 1000 << 11; // timestamp based write id generation can cause big write id jump.

      Connection          conn;
      Table               binlog_table;
      BlockingQueue<Cell> buffer;
      HBaseBinlog.RowKey  next_start_rowkey;
      HBaseBinlog.RowKey  next_stop_rowkey;
      int                 partialNum;
      boolean             running = true;
      int                 errorCounter = 0;

      public BinlogFetcher(Configuration conf, HBaseBinlog.RowKey currentHighestRowKey, int partialNum, final BlockingQueue<Cell> buffer) throws IOException {
         this.conn = ConnectionFactory.createConnection(conf);
         this.binlog_table = conn.getTable(HBaseBinlog.getTableName());
         this.next_start_rowkey = new HBaseBinlog.RowKey(currentHighestRowKey.getMapedSalt(), currentHighestRowKey.getWriteID() + 1, 0, (short)0);
         this.next_stop_rowkey = this.next_start_rowkey.clone();
         this.next_stop_rowkey.setWriteID(Long.MAX_VALUE);
         this.buffer = buffer;
         this.partialNum = partialNum;
         LOG.info("BinlogFetcher start fetch rowkey: " + next_start_rowkey);
      }

      public static void fetchAndPrintAll(Configuration conf) {
         try(Connection conn = ConnectionFactory.createConnection(conf);
             Table binlog_table = conn.getTable(HBaseBinlog.getTableName())) {
            final Scan scan = new Scan();
            scan.setStartRow(new HBaseBinlog.RowKey(0, 0, 0, (short)0).getRowArray());
            scan.addColumn(HBaseBinlog.getColumnFamily(), HBaseBinlog.getQualifier());
            try(ResultScanner scanner = binlog_table.getScanner(scan)) {
               SimpleDateFormat fmt = new SimpleDateFormat("MM/dd hh:mm:ss.SSS");
               for (final Result result : scanner) {
                  final List<Cell> cells = result.listCells();
                  for (final Cell cell : cells) {
                     final HBaseBinlog.RowKey rowKey = new HBaseBinlog.RowKey(CellUtil.cloneRow(cell));
                     String timestamp = fmt.format(new Date(cell.getTimestamp()));
                     if (rowKey.getWriteID() > 0) {
                        TransactionMutationMsg tmm = TransactionMutationMsg.parseFrom(CellUtil.cloneValue(cell));
                        System.out.println(String.format("%s, %s, commitID:%s, table:%s", timestamp, rowKey, tmm.getCommitId(), tmm.getTableName()));
                     }
                     else {
                        System.out.println(String.format("%s, %s", timestamp, rowKey));
                     }
                  }
               }
            }
         }
         catch (Exception e) {
            e.printStackTrace();
         }
      }

      @Override
      public void run() throws Exception {
         try {
             boolean has_result_before = true;
             boolean is_binlog_write_id_continuos = true;
             long realSalt = next_start_rowkey.getSalt(this.partialNum);
             HBaseBinlog.RowKey realBinlogCurrentHighestRowKey = HBaseBinlog.BinlogStats
                 .getBinlogStats(this.binlog_table, (int)realSalt,
                     this.partialNum).highest_rowkey;

             if (realBinlogCurrentHighestRowKey.getWriteID() < next_start_rowkey.getWriteID() - 1) {
                 LOG.error("realBinlogCurrentHighestRowKey <" + realBinlogCurrentHighestRowKey
                     .getWriteID() + ">, less than last_end_rowkey <" + (
                     next_start_rowkey.getWriteID() - 1) + ">, please check your environment");
             }
             while (running) {
                LOG.trace("BinlogFetcher next start rowkey (" + next_start_rowkey + ")");
                LOG.trace("BinlogFetcher next stop rowkey (" + next_stop_rowkey + ")");

                final Scan scan = new Scan();
                scan.setStartRow(next_start_rowkey.getRowArray());
                scan.setStopRow(next_stop_rowkey.getRowArray());
                scan.addColumn(HBaseBinlog.getColumnFamily(), HBaseBinlog.getQualifier());
                LOG.trace("BinlogFetcher scanning incremental : " + scan);

                boolean fetched_result = false;
                try(ResultScanner scanner = binlog_table.getScanner(scan)) {
                   for (final Result result : scanner) {
                      final List<Cell> cells = result.listCells();
                      for (final Cell cell : cells) {
                         final HBaseBinlog.RowKey rowKey = new HBaseBinlog.RowKey(CellUtil.cloneRow(cell));
                         if (rowKey.getWriteID() == next_start_rowkey.getWriteID()
                             || rowKey.getWriteID() - next_start_rowkey.getWriteID() > BIG_WID_JUMP) {
                            LOG.trace("BinlogFetcher put to buffer: " + rowKey);
                            buffer.put(cell);
                            next_start_rowkey.setWriteID(rowKey.getWriteID() + 1);
                         }
                         else if (rowKey.getWriteID() > next_start_rowkey.getWriteID()) {
                            LOG.warn(String.format("BinlogFetcher binlog write ID is not continuous, from %s jump to %s, try to re-fetch from write ID %s again.",
                                                   next_start_rowkey.getWriteID() - 1, rowKey.getWriteID(), next_start_rowkey.getWriteID()));
                            is_binlog_write_id_continuos = false;
                            break;
                         }
                         else {
                            LOG.error(String.format("BinlogFetcher duplicated binlog write ID %s fetched, binlog may in inconsistent state.", rowKey.getWriteID()));
                            throw new IOException("duplicated binlog write ID");
                         }
                      }

                      if (!is_binlog_write_id_continuos)
                         break;

                      fetched_result = true;
                      errorCounter = 0;
                   }
                }
                catch (final Exception e) {
                   LOG.error("Binlog Fetcher catch exception:", e);
                   //we now treat all HBase scan exception as retryable error, so just log and continue the loop to retry

                   //if here throw an exception, TaskDependentContext will catch it and stop the program
                   if( e.getMessage().contains("duplicated binlog write ID") ) //no retry error
                      throw e;
                   else
                      errorCounter++;
 
                   if(errorCounter > MAX_ERROR_COUNTER )
                      throw e;
                }

                if (!fetched_result) {
                   if (has_result_before) {
                      LOG.info("BinlogFetcher no new data fetched from source, periodic check new data with sleep interval " + WAIT_MASTER_MS + "ms , stop at " + next_start_rowkey);
                      has_result_before = false;
                   }
                   LOG.trace("BinlogFetcher no incremental binlog, sleep " + WAIT_MASTER_MS + "ms");
                   TimeUnit.MILLISECONDS.sleep(WAIT_MASTER_MS);
                }
                else {
                   if (!has_result_before) {
                      LOG.info("Binlog continue to fetch data");
                      has_result_before = true;
                   }
                }
             } //while loop
             LOG.info("BinlogFetcher exit");
         } catch (final Exception e) {
            LOG.error("Binlog Fetcher catch exception:", e);
         }
         finally {
            if (binlog_table != null) {
               binlog_table.close();
            }
            if (conn != null) {
               conn.close();
            }
         }
      }

      @Override
      public void stop() {
         running = false;
      }
   }

   static class BinlogWriter implements FailableTask {
      public static int CID_SORT_BUF_SIZE = 20;
      boolean running = true;
      Connection conn;
      Table binlog_table;
      BlockingQueue<Cell> buffer;

      public BinlogWriter(Configuration conf, BlockingQueue<Cell> buffer) throws IOException {
         LOG.debug("AsyncXDCWorker init binlog writer");
         conn = ConnectionFactory.createConnection(conf);
         binlog_table = conn.getTable(HBaseBinlog.getTableName());
         this.buffer = buffer;
      }

      @Override
      public void run() throws Exception {
         LOG.debug("AsyncXDCWorker BinlogWriter run");

         try {
            while (running) {
               Cell cell = buffer.take();
               writeLocalBinlog(cell, binlog_table);
            }
            LOG.info("AsyncXDCWorker BinlogWriter exit");
         }
         catch (final Exception e) {
            LOG.error("AsyncXDCWorker binlog writing with exception:  ", e);
            throw e;
         }
      }

      @Override
      public void stop() {
         running = false;
         try {
            if (binlog_table != null) {
               binlog_table.close();
               binlog_table = null;
            }

            if (conn != null) {
               conn.close();
               binlog_table = null;
            }
         } catch (Exception e) {
            LOG.error("AsyncXDCWorker binlog writer stop error:", e);
         }
      }

      private void writeLocalBinlog(final Cell cell, final Table binlogTable) throws IOException {
         final Put put = new Put(CellUtil.cloneRow(cell));
         put.add(cell);
         // TODO: using batch put to improve performance.
         binlogTable.put(put);
         if (LOG.isTraceEnabled()) {
            final RowKey rowkey = new RowKey(CellUtil.cloneRow(cell));
            LOG.trace("AsyncXDCWorker BinlogWriter new put rowkey:" + rowkey);
         }
      }
   }
}
