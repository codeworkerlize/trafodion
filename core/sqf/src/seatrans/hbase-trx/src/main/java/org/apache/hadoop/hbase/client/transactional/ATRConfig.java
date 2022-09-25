package org.apache.hadoop.hbase.client.transactional;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Scanner;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.transactional.utils.BashScript;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperListener;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.log4j.PropertyConfigurator;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.KeeperException.NoNodeException;
import org.apache.zookeeper.data.Stat;
import com.esgyn.common.LicenseHelper;


/**
 * ATR XDC Config
 *
 */
public class ATRConfig implements AutoCloseable {


   public static void setupConf() {
      String confFile = System.getProperty("trafodion.log4j.configFile");
      if (confFile == null) {
         System.setProperty("hostName", System.getenv("HOSTNAME"));
         System.setProperty("trafodion.atrxdc.log",
               System.getenv("TRAF_LOG") + "/trafodion.atrxdc.java.${hostName}.log");
         confFile = System.getenv("TRAF_CONF") + "/log4j.atrxdc.config";
      }
      PropertyConfigurator.configure(confFile);
   }

   static final Log LOG = LogFactory.getLog(ATRConfig.class);

   public static String XDC_CONF_PATH                    = "/trafodion/multi_dc";
   public static String ATR_CONF_NODE                    = "atr";
   public static String ROLE_NODE                        = "role";
   public static String SYNC_MODE_NODE                   = "syncmode";
   public static String DUAL_MODE_NODE                   = "dualmode";
   public static String BINLOG_NODE                      = "binlog";
   public static String INITIALIZED_NODE                 = "initialized";
   public static String PARTIAL_NUM_NODE                 = "partial";
   public static String BINLOG_SKIP_WAL_NODE             = "skipWAL";
   public static String SALT_MAP_NODE                    = "saltmap";
   public static String RECOVER_TIME_RANGE_NODE          = "recoverTimeRange";
   public static String WRITEID_FLUSH_INTERVAL_NODE      = "writeIDFlushInterval";
   public static String NEXT_WRITEID_JUMP_NODE           = "nextWriteIDJump";
   public static String WRITEID_FLUSH_CHECK_INTERVAL_NODE = "writeIDFlushCheckInterval";
   public static String BINLOG_WRITE_BUFFER_SIZE         = "mutatorbuf";
   public static String BINLOG_CONN_STR                  = "binlogconn";
   public static String THIRD_BINLOG_CONN_STR            = "thirdbinlogconn";
   public static String BINLOG_FLUSH_INTERVAL            = "binlogflushinvl";
   public static String CLUSTER_ROLE_PRIMARY             = "0";
   public static String CLUSTER_ROLE_SECONDARY           = "1";
   public final static String SYNC_MODE_MAX_PERFORMANCE  = "1";
   public final static String SYNC_MODE_MAX_RELIABILITY  = "2";
   public final static String SYNC_MODE_MAX_PROTECTION   = "3";
   public final static String SYNC_MODE_S_RELIABILITY    = "4";
   public static String ATRXDC_INITIALIZED               = "t";
   public static String WRITEID_FLUSH_CHECK_INTERVAL_DEFAULT   = "1";
   public static String ATRXDC_NOT_INITIALIZED           = "f";
   public static String BINLOG_SKIP_WAL_DEFAULT          = "f";
   public static String BINLOG_WRITE_BUFFER_SIZE_DEFAULT = "4194304";   //4M
   public static String ROLE_DEFAULT                     = CLUSTER_ROLE_PRIMARY;
   public static String SYNC_MODE_DEFAULT                = SYNC_MODE_S_RELIABILITY;
   public static boolean DUAL_MODE_DEFAULT               = false;
   public static String RECOVER_TIME_RANGE_DEFAULT       = "500";           // in seconds;
   public static String BINLOG_FLUSH_INTV_DEFAULT        = "1";             // in ms
   public static String LAST_COMMIT_TS_NODE              = "lastCommitTS";
   public static String WRITEID_FLUSH_INTERVAL_DEFAULT   = "10000";
   public static long NEXT_WRITEID_JUMP_DEFAULT          = 1;
   public static String SAFEBUFFER_LIMITER_NODE          = "safebufferLimiter";
   public static String SAFEBUFFER_LIMITER_DEFAULT       = "10000";
   public static String TOTALNUM_ALGO_NODE               = "totalnumalgo";
   public static String TOTALNUM_ALGO_DEFAULT            = "2";
   public static String BINLOG_MAX_FLUSH_CHECK_RETRIES   = "100";
   public static String BINLOG_MAX_FLUSH_CHECK_TIMES_NODE = "binlogfctimes";
   public static Integer LOG_TIME_LIMITER_DEFAULT       = 2000; //ms
   public static String LOG_TIME_LIMITER_NODE          = "logTimeLimiter";


   // option implementation
   private static class Option {
      public static ArrayList<Option> optionsArray;

      public int    seq;
      public String optionName;
      public String ZKPath;
      public String defaultValue;
      public String value;

      private ZooKeeperWatcher zkw;
      private Stat stat;

      public Option(final String optionName, final String defaultValue, final String ZKPath, final ZooKeeperWatcher zkw) {
         this.seq          = globalSeq++;
         this.optionName   = optionName;
         this.ZKPath       = ZKPath;
         this.defaultValue = defaultValue;
         this.zkw          = zkw;
         this.stat         = new Stat();

         if (optionsArray == null) {
            optionsArray = new ArrayList<>();
         }

         optionsArray.add(this);
      }

      public String getValue() throws KeeperException, InterruptedException, IOException {
         if (value == null) {
            if (ZKUtil.checkExists(zkw, ZKPath) != -1)
               value = new String(ZKUtil.getDataAndWatch(zkw, ZKPath, stat));
            else
               throw new IOException(String.format("ZK path %s not exist, did you run atrxdc_init?", ZKPath));
            LOG.info(String.format("ATRConfig get zk value: %s - %s", ZKPath, value));
         }
         return value;
      }

      public void setValue(final String value) throws KeeperException {
         ZKUtil.createSetData(zkw, ZKPath, value.getBytes());
         LOG.info(String.format("ATRConfig set zk value: %s - %s", ZKPath, value));
      }

      public void clearValue() {
         value = null;
      }

      public String toString() {
         try {
            return String.format("option name: %s, zk path: %s, default values: %s, current value: %s",
                                 optionName, ZKPath, defaultValue, getValue());
         } catch (KeeperException | InterruptedException | IOException e) {
            LOG.trace("ATRConfig to string error: " + e);
            return e.getMessage();
         }
      }

      private static int globalSeq;
   }

   // *** online configable options ***
   private final Option roleOption;
   private final Option syncModeOption;
   private final Option dualModeOption;
   private final Option initializedOption;
   private final Option binlogPartialNumOption;
   private final Option binlogSkipWALOption;
   private final Option recoverTimeRangeOption;
   private final Option writeIDFlushIntervalOption;
   private final Option writeIDFlushCheckIntervalOption;
   // *********************************
   private final Option binlogSaltMapOption;
   private final Option binlogWriterBufferSize;
   private final Option binlogConnStr;
   private final Option thirdBinlogConnStr;
   private final Option binlogFlushIntervalOption;
   private final Option safeBufferLimiterOption;
   private final Option totalNumAlgoOption;
   private final Option flushCheckMaxRetriesOption;


   private final Option logTimeLimiterOption;


   private final ZooKeeperWatcher zkw;
   private final String binlogZKPath;
 
   private static boolean isLicenseMatch = false;

   private static class Singleton {
      private static ATRConfig instance;

      static {
         try {
            final Configuration conf = HBaseConfiguration.create();
            instance = new ATRConfig(conf);
            isLicenseMatch = LicenseHelper.isAdvancedEdition() && LicenseHelper.isModuleOpen(LicenseHelper.Modules.BINLOG);
         } catch (IOException | KeeperException | InterruptedException e) {
            LOG.error("ATRConfig initialize singleton error: ", e);
         }
      }
   }

   private ATRConfig(final Configuration conf) throws ZooKeeperConnectionException, IOException, KeeperException, InterruptedException {
      objectsReinitializeOnConfigChange = new LinkedList<>();
      zkw = new ZooKeeperWatcher(conf, "ATRConfig", null);

      final String ATR_CONF_PATH             = XDC_CONF_PATH + "/" + ATR_CONF_NODE;
      binlogZKPath                = ATR_CONF_PATH + "/" + BINLOG_NODE;
      roleOption                  = new Option("clusterRole", ROLE_DEFAULT, ATR_CONF_PATH + "/" + ROLE_NODE, zkw);
      syncModeOption              = new Option("syncMode", SYNC_MODE_DEFAULT, ATR_CONF_PATH + "/" + SYNC_MODE_NODE, zkw);
      dualModeOption              = new Option("dualMode", String.valueOf(DUAL_MODE_DEFAULT), ATR_CONF_PATH + "/" + DUAL_MODE_NODE, zkw);
      initializedOption           = new Option("ATRConfigInitialized", ATRXDC_NOT_INITIALIZED, binlogZKPath + "/" + INITIALIZED_NODE, zkw);
      binlogPartialNumOption      = new Option("binlogPartialNum", null, binlogZKPath + "/" + PARTIAL_NUM_NODE, zkw);
      binlogSkipWALOption         = new Option("binlogSkipWAL", BINLOG_SKIP_WAL_DEFAULT, binlogZKPath + "/" + BINLOG_SKIP_WAL_NODE, zkw);
      binlogSaltMapOption         = new Option("binlogSaltMapOption", null, binlogZKPath + "/" + SALT_MAP_NODE, zkw);
      binlogWriterBufferSize      = new Option("binlogMutatorWriterBufferSize", BINLOG_WRITE_BUFFER_SIZE_DEFAULT, binlogZKPath + "/" + BINLOG_WRITE_BUFFER_SIZE, zkw);
      binlogConnStr               = new Option("binlogConnectionString", "", binlogZKPath + "/" + BINLOG_CONN_STR, zkw);
      thirdBinlogConnStr          = new Option("thirdBinlogConnectionString", "", binlogZKPath + "/" + THIRD_BINLOG_CONN_STR, zkw);
      binlogFlushIntervalOption   = new Option("binlogFlushIntervalString", BINLOG_FLUSH_INTV_DEFAULT, binlogZKPath + "/" + BINLOG_FLUSH_INTERVAL, zkw);
      recoverTimeRangeOption      = new Option("recoverTimeRange", RECOVER_TIME_RANGE_DEFAULT, binlogZKPath + "/" + RECOVER_TIME_RANGE_NODE, zkw);
      writeIDFlushIntervalOption  = new Option("writeIDFlushInterval", WRITEID_FLUSH_INTERVAL_DEFAULT, binlogZKPath + "/" + WRITEID_FLUSH_INTERVAL_NODE, zkw);
      writeIDFlushCheckIntervalOption  = new Option("writeIDFlushCheckInterval", WRITEID_FLUSH_CHECK_INTERVAL_DEFAULT, binlogZKPath + "/" + WRITEID_FLUSH_CHECK_INTERVAL_NODE, zkw);
      safeBufferLimiterOption     = new Option("safeBufferLimiter", SAFEBUFFER_LIMITER_DEFAULT, binlogZKPath + "/" + SAFEBUFFER_LIMITER_NODE, zkw);
      totalNumAlgoOption  = new Option("totalNumAlgo", TOTALNUM_ALGO_DEFAULT , binlogZKPath + "/" + TOTALNUM_ALGO_NODE , zkw);
      flushCheckMaxRetriesOption = new Option("maxFlushCheckRetries", BINLOG_MAX_FLUSH_CHECK_RETRIES , binlogZKPath + "/" + BINLOG_MAX_FLUSH_CHECK_TIMES_NODE, zkw);

      logTimeLimiterOption        = new Option("logTimeLimiter", String.valueOf(LOG_TIME_LIMITER_DEFAULT),
              binlogZKPath + "/" + LOG_TIME_LIMITER_NODE, zkw);


      if (ZKUtil.checkExists(zkw, initializedOption.ZKPath) == -1) {
         ZKUtil.createWithParents(zkw, initializedOption.ZKPath, initializedOption.defaultValue.getBytes());
      }

      this.new OptionsListener(zkw);
   }

   public static ATRConfig instance() {
      return Singleton.instance;
   }

   public static ATRConfig newInstance(final Configuration conf) throws ZooKeeperConnectionException, IOException, KeeperException, InterruptedException {
      return new ATRConfig(conf);
   }

   public static String getReplayerCheckPointPath() {
      return XDC_CONF_PATH + "/" + ATR_CONF_NODE + "/" + BINLOG_NODE + "/" + "CP";
   }

   public static String getNextWriteIDJumpNode() {
      return XDC_CONF_PATH + "/" + ATR_CONF_NODE + "/" + BINLOG_NODE + "/" + NEXT_WRITEID_JUMP_NODE;
   }

   public static String getNextWriteIDJumpPath(int salt) {
      return getNextWriteIDJumpNode() + "/" + salt;
   }

   public String getRole() throws KeeperException, InterruptedException, IOException {
      return roleOption.getValue();
   }

   public void setRole(final String role) throws NoNodeException, KeeperException {
      roleOption.setValue(role);
   }

   public String getSyncMode() {
      try {
         return syncModeOption.getValue();
      } catch (KeeperException | InterruptedException | IOException e) {
         return SYNC_MODE_DEFAULT;
      }
   }

   public void setSyncMode(final String mode) throws NoNodeException, KeeperException {
      syncModeOption.setValue(mode);
      System.out.println("***WARN***: operation success, restart EsgynDB to take affect.");
   }

   public boolean isDualMode() {
      try {
         return Boolean.valueOf(dualModeOption.getValue());
      } catch (KeeperException | InterruptedException | IOException e) {
         LOG.warn("ATRConfig get dual mode failed: ", e);
         return false;
      }

   }

   public void setDualMode(final boolean isDual) throws NoNodeException, KeeperException {
      dualModeOption.setValue(String.valueOf(isDual));
   }

   public String getBinlogPartNum() throws KeeperException, InterruptedException, IOException {
      return binlogPartialNumOption.getValue();
   }

   public void setBinlogPartNum(final String binlogPartNum) throws KeeperException {
      binlogPartialNumOption.setValue(binlogPartNum);
   }

   public String getBinlogPartNumZKPath() {
      return binlogPartialNumOption.ZKPath;
   }

   public boolean isBinlogSkipWAL() {
      try {
         return binlogSkipWALOption.getValue().equals("t");
      }
      catch (Exception e) {
         LOG.error("ATRConfig get skip WAL option failed with error:", e);
         return false;
      }
   }

   public void setBinlogSkipWAL(boolean skipWAL) throws KeeperException {
      binlogSkipWALOption.setValue(skipWAL ? "t" : "f");
   }

   public void setBinlogFlushInterval (final String invl ) throws KeeperException {
      binlogFlushIntervalOption.setValue(invl);
   }

   public String getBinlogFlushInterval () throws KeeperException, InterruptedException, IOException {
      return binlogFlushIntervalOption.getValue();
   }

   public long getRecoverTimeRange(){
      try {
         return Long.valueOf(recoverTimeRangeOption.getValue());
      } catch (Exception e) {
         return Long.valueOf(RECOVER_TIME_RANGE_DEFAULT);
      }
   }

   public void setRecoverTimeRange(String timeRange) throws KeeperException {
      recoverTimeRangeOption.setValue(timeRange);
   }

   public long getWriteIDFlushInterval(){
      try {
         return Long.valueOf(writeIDFlushIntervalOption.getValue());
      } catch (Exception e) {
         return Long.valueOf(WRITEID_FLUSH_INTERVAL_DEFAULT);
      }
   }

   public void setWriteIDFlushInterval(String interval) throws KeeperException {
      writeIDFlushIntervalOption.setValue(interval);
   }


   public int getWriteIDFlushCheckInterval(){
      try {
         return Integer.valueOf(writeIDFlushCheckIntervalOption.getValue());
      } catch (Exception e) {
         return Integer.valueOf(WRITEID_FLUSH_CHECK_INTERVAL_DEFAULT);
      }
   }

   public void setWriteIDFlushCheckInterval(String interval) throws KeeperException {
      writeIDFlushCheckIntervalOption.setValue(interval);
   }

   public long getNextWriteIDJump(int salt){
      try {
         byte[] data = ZKUtil.getData(zkw, getNextWriteIDJumpPath(salt));
         if (data == null) {
            LOG.info("ATRConfig get next write id jum return default value, due to null data in path " + getNextWriteIDJumpPath(salt));
            return NEXT_WRITEID_JUMP_DEFAULT;
         }
         return Bytes.toLong(data);
      } catch (Exception e) {
         LOG.warn("ATRConfig get next write id jump return default value, due to exception: ", e);
         return NEXT_WRITEID_JUMP_DEFAULT;
      }
   }

   public void setNextWriteIDJump(int salt, long nextJump) throws KeeperException {
      ZKUtil.createSetData(zkw, getNextWriteIDJumpPath(salt), Bytes.toBytes(nextJump));
   }

   public void clearNextWriteIDJump() throws KeeperException {
      ZKUtil.deleteChildrenRecursively(zkw, ATRConfig.getNextWriteIDJumpNode());
   }

   public String getBinlogMutatorWriteBufferSize() throws KeeperException, InterruptedException, IOException {
      return binlogWriterBufferSize.getValue();
   }

   public void setBinlogMutatorWriteBufferSize(final String writeBuffSize) throws KeeperException {
      binlogWriterBufferSize.setValue(writeBuffSize);
   }

   public String getThirdBinlogConnectionString() throws KeeperException, InterruptedException, IOException {
      return thirdBinlogConnStr.getValue();
   }

   public void setThirdBinlogConnectionString(final String connstr) throws KeeperException {
      thirdBinlogConnStr.setValue(connstr);
   }

   public String getBinlogConnectionString() throws KeeperException, InterruptedException, IOException {
      return binlogConnStr.getValue();
   }

   public void setBinlogConnectionString(final String connstr) throws KeeperException {
      binlogConnStr.setValue(connstr);
   }

   public void setSaltMap(String saltMap) throws Exception {
      binlogSaltMapOption.setValue(saltMap);
   }

   public String getSaltMap() throws Exception {
      return binlogSaltMapOption.getValue();
   }

   public void setSafebufferLimiter(final String safebufferLimiter) throws KeeperException {
      safeBufferLimiterOption.setValue(safebufferLimiter);
   }

   public int getSafebufferLimiter() throws KeeperException, InterruptedException, IOException{
      try {
         return Integer.valueOf(safeBufferLimiterOption.getValue());
      } catch (Exception e) {
         return Integer.valueOf(SAFEBUFFER_LIMITER_DEFAULT);
      }
   }

   public void setTotalNumAlgo(final String algo) throws KeeperException {
      totalNumAlgoOption.setValue(algo);
   }

   public int getTotalNumAlgo() throws KeeperException, InterruptedException, IOException{
      return Integer.valueOf(totalNumAlgoOption.getValue());
   }

   public void setMaxBinlogFCTimes(final String t) throws KeeperException {
      flushCheckMaxRetriesOption.setValue(t);
   }

   public int getMaxBinlogFCTimes() throws KeeperException, InterruptedException, IOException{
      return Integer.valueOf(flushCheckMaxRetriesOption.getValue());
   }

   public String getBinlogZKPath() {
      return binlogZKPath;
   }

   public boolean isMaxPerformanceSyncMode() {
      return SYNC_MODE_MAX_PERFORMANCE.equals(getSyncMode());
   }

   public boolean isMaxProtectionSyncMode() {
      return SYNC_MODE_MAX_PROTECTION.equals(getSyncMode());
   }

   public boolean isStrictReliabilitySyncMode() {
      return SYNC_MODE_S_RELIABILITY.equals(getSyncMode());
   }

   public boolean isATRXDCEnabled() {
      try {
         return  initializedOption.getValue().equals("t");
      }
      catch (Exception e) {
         LOG.warn("ATRConfig check if atrxdc enabled error: " + e);
      }
      return false;
   }

   public boolean isPrimaryCluster() {
      try {
         return getRole().equals(CLUSTER_ROLE_PRIMARY);
      } catch (KeeperException | InterruptedException | IOException e) {
         LOG.warn("ATRConfig get role failed: ", e);
         return true;
      }
   }

   public boolean isRemote() {
      try {
         return (getBinlogConnectionString() != null && !getBinlogConnectionString().isEmpty());
      } catch (KeeperException | InterruptedException | IOException e) {
         LOG.warn("ATRConfig get connection string failed: ", e);
         return false;
      }
   }
   public static String getClusterNodeCount() throws IOException {
      BashScript bashScript = new BashScript("trafconf -nid-count");
      try {
         bashScript.execute();
         return bashScript.stdout();
      } catch (Exception e) {
         LOG.error(String.format("get cluster node count error, caused by bash command failed: %s, context: %s\n",
                                 bashScript, bashScript.get_execution_context()));
         throw new IOException("Can't get cluster node count");
      }
   }

   public void initializeDefaultConfig() throws KeeperException, IOException {
      if(isLicenseMatch == false) //you cannot use binlog
      {
        LOG.error("You don't have valid License to enable binlog feature");
        throw new IOException("You don't have valid License to enable binlog feature");
      }
      for (final Option oh : Option.optionsArray) {
         if (oh.defaultValue != null)
            ZKUtil.createWithParents(zkw, oh.ZKPath, oh.defaultValue.getBytes());
      }

      setBinlogPartNum(getClusterNodeCount());
      clearNextWriteIDJump();
   }

   public void enableATRXDC(boolean enable) throws KeeperException, IOException {
      if(isLicenseMatch == false) //you cannot use binlog
      {
        LOG.error("You don't have valid License to enable binlog feature");
        throw new IOException("You don't have valid License to enable binlog feature");
      }
      initializedOption.setValue(enable ? ATRXDC_INITIALIZED : ATRXDC_NOT_INITIALIZED);
   }

   public void setLastCommitTS(int saltNum, long commitTS) throws KeeperException {
      String zkPath = binlogZKPath + "/" + LAST_COMMIT_TS_NODE + "/" + saltNum;
      ZKUtil.createSetData(zkw, zkPath, String.valueOf(commitTS).getBytes());
   }

   public long getLastCommitTS(int saltNum) {
      String zkPath = binlogZKPath + "/" + LAST_COMMIT_TS_NODE + "/" + saltNum;
      try {
         return Long.parseLong(new String(ZKUtil.getData(zkw, zkPath)));
      } catch (Exception e) {
         LOG.warn("Failed to get last commit timestamps from ZK.", e);
         return 0l;
      }
   }

   // *** register objects that should be reinitialize on config change ***
   public static interface ReInitializable {
      void reInitialize();
   }
   private List<ReInitializable> objectsReinitializeOnConfigChange;

   public void registerReinitializeOnConfigChange(ReInitializable obj) {
      if (!objectsReinitializeOnConfigChange.contains(obj)) {
         objectsReinitializeOnConfigChange.add(obj);
      }
   }

   private void reinitialize() {
      for (final Option oh : Option.optionsArray) {
         oh.clearValue();
      }
   }

   private void reinitializeRegisterObjects() {
      for (ReInitializable obj : objectsReinitializeOnConfigChange) {
         obj.reInitialize();
      }
   }

   public Integer getLogTimeLimiter() {
      try {
         return Integer.valueOf(logTimeLimiterOption.getValue());
      } catch (Exception e){
         return LOG_TIME_LIMITER_DEFAULT;
      }
   }

   public void setLogTimeLimiter(String logTimeLimiter) throws KeeperException {
      logTimeLimiterOption.setValue(String.valueOf(logTimeLimiter));
   }

   public class OptionsListener extends ZooKeeperListener {

      public OptionsListener(final ZooKeeperWatcher watcher) {
         super(watcher);
         watcher.registerListener(this);
      }

      @Override
      public void nodeDataChanged(final String path) {
         System.out.println("node changed: " + path);

         if (path.equals(initializedOption.ZKPath)) {
            reinitialize();
            reinitializeRegisterObjects();
            try {
               LOG.info("ATRConfig reinitialized to: " + initializedOption.getValue());
            } catch (KeeperException | InterruptedException | IOException e) {
               LOG.error("ATRConfig initializeOption get value error: " + e);
            }
         }
         else {
            for (Option op : Option.optionsArray) {
               if (op.ZKPath.equals(path)) {
                  op.clearValue();
                  break;
               }
            }
         }
      }
   }

   @Override
   public void close() {
      if (zkw != null) {
         zkw.close();
      }
   }

   public String toString() {
      StringBuilder builder = new StringBuilder();
      if (isATRXDCEnabled()) {
         for (Option op : Option.optionsArray) {
            builder.append(op.toString()).append("\n");
         }
      }
      else {
         builder.append("ATRXDC configuration not initialized");
      }
      return builder.toString();
   }

   public static void clearExistsOptions() {
      Option.optionsArray.clear();
   }

   public static void main(final String[] args) throws KeeperException, InterruptedException, IOException {
      ATRConfig.setupConf();
      try (ATRConfig atrconf = ATRConfig.instance()) {
         if (args.length == 0) {
            ReInitializable testObj = new ReInitializable(){
                  @Override
                  public void reInitialize() {
                     System.out.println("test object reinitialized.");
                  }
               };
            atrconf.registerReinitializeOnConfigChange(testObj);
            System.out.println("input p to print config other to to exit");
            try(Scanner scan = new Scanner(System.in)) {
               while (scan.hasNext()) {
                  String line = scan.nextLine();
                  if (line.startsWith("p")) {
                     System.out.println(atrconf.toString());
                  }
                  else break;
               }
            }
            return ;
         }

         switch (args[0]) {
         case "init":
            if(isLicenseMatch == false) //you cannot use binlog
            {
              System.out.println("atrxdc configuration initialized failed, license is not allowed to init binlog.");
              throw new IOException("atrxdc configuration initialized failed, license is not allowed to init binlog.");
            }
            atrconf.initializeDefaultConfig();
            System.out.println("atrxdc configuration initialized.");
            break;
         case "enable":
            if(isLicenseMatch == false) //you cannot use binlog
            {
              System.out.println("atrxdc enable failed, license is not allowed to enable binlog.");
              break;
            }
            atrconf.enableATRXDC(true);
            break;
         case "disable":
            atrconf.enableATRXDC(false);
            break;
         case "setrole":
            atrconf.setRole(args[1].equals("secondary") ? CLUSTER_ROLE_SECONDARY : CLUSTER_ROLE_PRIMARY);
            break;
         case "getrole":
            try {
               System.out.println(atrconf.getRole().equals(CLUSTER_ROLE_SECONDARY) ? "secondary" : "primary");
            } catch (Exception e) {
               System.out.println("primary");
            }
            break;
         case "setmode":
            atrconf.setSyncMode(args[1]);
            break;
         case "getmode":
            String mode = "";
            switch (atrconf.getSyncMode()) {
               case "1":
                  mode = "MAX_PERFORMANCE_MODE";
                  break;
               case "2":
                  mode = "MAX_RELIABILITY_MODE";
                  break;
               case "3":
                  mode = "MAX_PROTECTION_MODE";
                  break;
               case "4":
                  mode = "MAX_S_RELIABILITY_MODE";
                  break;
            }
            System.out.println(mode);
            break;
         case "setdual":
            atrconf.setDualMode(Boolean.valueOf(args[1]));
            break;
         case "isDual":
            System.out.println(atrconf.isDualMode());
            break;
         case "setpartnum":
            atrconf.setBinlogPartNum(args[1]);
            break;
         case "getpartnum":
            System.out.println(atrconf.getBinlogPartNum());
            break;
         case "setbinlogskipwal":
            atrconf.setBinlogSkipWAL(args[1].equalsIgnoreCase("t") || args[1].equalsIgnoreCase("true"));
            break;
         case "getbinlogskipwal":
            System.out.println(atrconf.isBinlogSkipWAL() ? "true" : "false");
            break;
         case "setbinlogflushintvl":
            atrconf.setBinlogFlushInterval(args[1]);
            break;
         case "getbinlogflushintvl":
            String intv = atrconf.getBinlogFlushInterval();
            if( intv.equals("") )
              System.out.println("500");
            else
              System.out.println(atrconf.getBinlogFlushInterval());
            break;
         case "setrecovertimerange":
            atrconf.setRecoverTimeRange(args[1]);
            break;
         case "getrecovertimerange":
            System.out.println(atrconf.getRecoverTimeRange());
            break;
         case "setwidflushinterval":
            atrconf.setWriteIDFlushInterval(args[1]);
            break;
         case "getwidflushinterval":
            System.out.println(atrconf.getWriteIDFlushInterval());
            break;
         case "setflushchkinterval":
            atrconf.setWriteIDFlushCheckInterval(args[1]);
            break;
         case "getflushchkinterval":
            System.out.println(atrconf.getWriteIDFlushCheckInterval());
            break;
         case "setnextwidjump":
            atrconf.setNextWriteIDJump(Integer.valueOf(args[1]), Long.valueOf(args[2]));
            break;
         case "setflushchktimes":
            atrconf.setMaxBinlogFCTimes(args[1]);
            break;
         case "getflushchktimes":
            System.out.println(atrconf.getMaxBinlogFCTimes());
            break;
         case "getnextwidjump":
            System.out.println(atrconf.getNextWriteIDJump(Integer.valueOf(args[1])));
            break;
         case "setbinlogbufsize":
            atrconf.setBinlogMutatorWriteBufferSize(args[1]);
            break;
         case "getbinlogbufsize":
            System.out.println(atrconf.getBinlogMutatorWriteBufferSize());
            break;
         case "setbinlogconn":
            if(args.length <= 1)
              atrconf.setBinlogConnectionString("");
            else
              atrconf.setBinlogConnectionString(args[1]);
            break;
         case "getbinlogconn":
            System.out.println(atrconf.getBinlogConnectionString());
            break;
         case "setthirdbinlogconn":
            atrconf.setThirdBinlogConnectionString(args[1]);
            break;
         case "getthirdbinlogconn":
            System.out.println(atrconf.getThirdBinlogConnectionString());
            break;
         case "setsblimiter":
            atrconf.setSafebufferLimiter(args[1]);
            break;
         case "getsblimiter":
            System.out.println(atrconf.getSafebufferLimiter());
            break;
         case "settnalgo":
            atrconf.setTotalNumAlgo(args[1]);
            break;
         case "gettnalgo":
            System.out.println(atrconf.getTotalNumAlgo());
            break;
         case "check":
            if (args.length > 1) {
               if (args[1].equals("remote")) {
                  Configuration conf = HBaseConfiguration.create();
                  if (atrconf.getBinlogConnectionString() == null || atrconf.getBinlogConnectionString().trim().isEmpty()) {
                     System.out.println("remote binlog connection string is empty.");
                  }
                  else {
                     conf.set("hbase.zookeeper.quorum", atrconf.getBinlogConnectionString());
                     clearExistsOptions();
                     ATRConfig ratrconf = ATRConfig.newInstance(conf);
                     System.out.println("show configuration of remote: " + atrconf.getBinlogConnectionString());
                     System.out.println(ratrconf.toString());
                  }
               }
               else {
                  System.out.println("unsupported check option: " + args[1]);
                  return;
               }
            }
            else {
               System.out.println(atrconf.toString());
            }
            break;
         case "setlogtimelimiter":
            atrconf.setLogTimeLimiter(args[1]);
            break;
         case "getlogtimelimiter":
            System.out.println(atrconf.getLogTimeLimiter());
            break;
         default:
            System.out.println("unsupported command: " + args[0]);
         }
      }
   }
}
