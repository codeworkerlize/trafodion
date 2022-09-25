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

package org.trafodion.sql;

import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.TreeMap;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import org.apache.log4j.PropertyConfigurator;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;

import org.apache.hadoop.util.StringUtils;

import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;

// These are needed for the DDL_TIME constant. This class is different in Hive 0.10.
// We use Java reflection instead of importing the class statically. 
// For Hive 0.9 or lower
// import org.apache.hadoop.hive.metastore.api.Constants;
// For Hive 0.10 or higher
// import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.BlockLocation;

import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.KeeperException;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

import org.apache.zookeeper.ZooKeeper;

import org.trafodion.sql.DistributedLock;
import org.trafodion.sql.TrafConfiguration;

import com.esgyn.security.util.SentryPolicyService;
import com.esgyn.security.authorization.EsgynDBPrivilege;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.Executors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ExecutorCompletionService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import org.apache.orc.OrcFile;
import org.apache.orc.Reader;
import org.apache.orc.StripeInformation;
import java.lang.management.ManagementFactory;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.BlockMetaData;

import java.lang.management.RuntimeMXBean;
import org.apache.log4j.MDC;

public class HiveClient {
    public static final int Table_TABLE_NAME = 0;
    public static final int Table_DB_NAME = 1;
    public static final int Table_OWNER = 2;
    public static final int Table_CREATE_TIME = 3;
    public static final int Table_TABLE_TYPE = 4;
    public static final int Table_VIEW_ORIGINAL_TEXT = 5;
    public static final int Table_VIEW_EXPANDED_TEXT = 6;
    public static final int Table_SD_COMPRESSED = 7;
    public static final int Table_SD_LOCATION = 8;
    public static final int Table_SD_INPUT_FORMAT = 9;
    public static final int Table_SD_OUTPUT_FORMAT = 10;
    public static final int Table_SD_NUM_BUCKETS = 11;
    public static final int Table_NULL_FORMAT = 12;
    public static final int Table_FIELD_DELIM = 13;
    public static final int Table_LINE_DELIM = 14;
    public static final int Table_SERIALIZE_LIB = 15;
    public static final int Table_FIELD_COUNT  = 16;

    // enums are in common/ComSmallDefs.h
    private static final int ORC = 3;
    private static final int PARQUET = 4;
    
    public static final int Col_NAME = 0;
    public static final int Col_TYPE = 1;
    public static final int Col_FIELD_COUNT  = 2;

    private static Logger logger = Logger.getLogger(HiveClient.class.getName());
    private static HiveConf hiveConf = null;
    private static ThreadLocal<HiveMetaStoreClient> hiveMetaClient  ;
    private static String ddlTimeConst = null;
    private static int sessionTimeout = 3000;
    private static int numThreads = 10; 
  
    private static Configuration config = null;
    private static Statement stmt = null;

    private static ThreadLocal<ExecutorService> m_executorService = null;
    private int partNum = 0;
    static {
         System.setProperty("hostName", System.getenv("HOSTNAME"));
         String confFile = System.getProperty("trafodion.log4j.configFile");
         System.setProperty("trafodion.root", System.getenv("TRAF_HOME"));
         if (confFile == null) 
            confFile = System.getenv("TRAF_CONF") + "/log4j.sql.config";
         PropertyConfigurator.configure(confFile);
         hiveConf = (HiveConf)TrafConfiguration.create(TrafConfiguration.HIVE_CONF);
         numThreads = hiveConf.getInt("hive.num.filestats.read.threads", 10);
         RuntimeMXBean rt = ManagementFactory.getRuntimeMXBean();
         String pid = rt.getName();
         MDC.put("PID", pid);
         hiveMetaClient = new ThreadLocal<HiveMetaStoreClient>();
         m_executorService = new ThreadLocal<ExecutorService>();
         try {
             ddlTimeConst = getDDLTimeConstant();
         } catch (MetaException me)
         {
             throw new RuntimeException("Checked MetaException from HiveClient static block");
         }
    }
 
    HiveClient() {
    }

   private static HiveMetaStoreClient getHiveMetaClient() throws org.apache.hadoop.hive.metastore.api.MetaException 
   {
       HiveMetaStoreClient ts_hmsClient;
       ts_hmsClient = hiveMetaClient.get(); 
       if (ts_hmsClient == null) {
          ts_hmsClient = new HiveMetaStoreClient(hiveConf, null);
          hiveMetaClient.set(ts_hmsClient);
       }
       return ts_hmsClient;
   }  

   private static ExecutorService getExecutorService() throws IOException 
   {
      ExecutorService ts_executorService;
      ts_executorService = m_executorService.get();
      if (ts_executorService == null) {
         ts_executorService = Executors.newFixedThreadPool(numThreads);
         m_executorService.set(ts_executorService);
      }
      return ts_executorService;
   }
   
   public static boolean close() throws InterruptedException
   {
        if (logger.isDebugEnabled()) logger.debug("HiveClient.close called.");
        return true;
   }

    public static boolean exists(String schName, String tblName)  
        throws MetaException, TException, UnknownDBException 
    {
        if (logger.isDebugEnabled()) logger.debug("HiveClient.exists(" + schName + " , " + tblName + ") called.");
        boolean result = getHiveMetaClient().tableExists(schName, tblName);
        return result;
    }

    public static long getRedefTime(String schName, String tblName, boolean allowSubdir)
        throws MetaException, TException, IOException
    {
        Table table;
        long modificationTime;
        if (logger.isDebugEnabled()) logger.debug("HiveClient.getRedefTime(" + schName + " , " + 
                     tblName + ") called.");
        try {
            table = getHiveMetaClient().getTable(schName, tblName);
        }
        catch (NoSuchObjectException x) {
            if (logger.isDebugEnabled()) logger.debug("Hive table no longer exists.");
            return 0;
        }
        long redefTime = table.getCreateTime();
        if (table.getParameters() != null){
            // those would be used without reflection
            //String rfTime = table.getParameters().get(Constants.DDL_TIME);
            //String rfTime = table.getParameters().get(hive_metastoreConstants.DDL_TIME);
            // determing the constant using reflection instead
            String rfTime = table.getParameters().get(ddlTimeConst);
            if (rfTime != null)
                redefTime = Long.parseLong(rfTime);
        }
        // createTime is in seconds
        // Assuming DDL_TIME is also in seconds
        redefTime *= 1000;
        // Get the lastest partition/file timestamp 
        int levelDeep = allowSubdir ? -1 : table.getPartitionKeysSize();
        String rootDir = table.getSd().getLocation();
        long dirTime = 0;
        if (rootDir != null) {
           try {
	       dirTime = HDFSClient.getHiveTableMaxModificationTs(rootDir, levelDeep);
           } catch (FileNotFoundException e) {
           // ignore this exception
           }
        }
        if (dirTime > redefTime)
           modificationTime = dirTime;
        else
           modificationTime = redefTime;
        if (logger.isDebugEnabled()) logger.debug("RedefTime is " + redefTime);
        return modificationTime;
    }

    public static Object[] getAllSchemas() throws MetaException 
    {
        List<String> schemaList = (getHiveMetaClient().getAllDatabases());
        if (schemaList != null)
           return schemaList.toArray();
        else
           return null; 
    }

    public static Object[] getAllTables(String schName) 
        throws MetaException, TException 
    {
        try {
        Database db = getHiveMetaClient().getDatabase(schName);
        if (db == null)
            return null;

        List<String> tableList = getHiveMetaClient().getAllTables(schName);
        if (tableList != null)
           return tableList.toArray();
        else
           return null;
        } catch (NoSuchObjectException e) {
          return null;
        }
    }

    public static Object[] getAllTablesMeta(String schName) 
        throws MetaException, TException {

        try {
	    Database db = getHiveMetaClient().getDatabase(schName);
	    if (db == null)
		return null;
	    
	    List<String> tableStrList = getHiveMetaClient().getAllTables(schName);
	    if (tableStrList == null)
		return null;
	    
	    List<Table> tableList = 
		getHiveMetaClient().getTableObjectsByName(schName,tableStrList);
	    if (tableList != null) {
		List<String> tabTypeList = new ArrayList<String>();
		for (Table obj : tableList) {
		    tabTypeList.add(obj.getTableName() + 
				    ", TableType: " + obj.getTableType());
		}
		return tabTypeList.toArray();
	    }
	    else
		return null;
	} catch (NoSuchObjectException e) {
	    return null;
        }
    }

    public static Object[] getAllViews(String schName) 
        throws MetaException, TException {

	try {
	    Database db = getHiveMetaClient().getDatabase(schName);
	    if (db == null)
		return null;
	    
	    List<String> tableStrList = getHiveMetaClient().getAllTables(schName);
	    if (tableStrList == null)
		return null;
	    
	    List<Table> tableList = 
		getHiveMetaClient().getTableObjectsByName(schName,tableStrList);
	    if (tableList != null) {
		List<String> viewList = new ArrayList<String>();
		for (Table obj : tableList) {
		    if (obj.getTableType().contains("VIEW"))
			viewList.add(obj.getTableName());
		}
		return viewList.toArray();
	    }
	    else
		return null;
	} catch (NoSuchObjectException e) {
          return null;
        }
    }

    // Because Hive changed the name of the class containing internal constants changed
    // in Hive 0.10, we are using Java Reflection to get the value of the DDL_TIME constant.
    public static String getDDLTimeConstant()
        throws MetaException 
    {

        Class constsClass = null;
        Object constsFromReflection = null; 
        Field ddlTimeField = null;
        Object fieldVal = null;

        // Using the class loader, try to load either class by name.
        // Note that both classes have a default constructor and both have a static
        // String field DDL_TIME, so the rest of the code is the same for both.
        try { 
            try {
                constsClass = Class.forName(
                   // Name in Hive 0.10 and higher
                   "org.apache.hadoop.hive.metastore.api.hive_metastoreConstants");
            } catch (ClassNotFoundException e) { 
                // probably not found because we are using Hive 0.10 or later
                constsClass = null;
            } 
            if (constsClass == null) {
                constsClass = Class.forName(
                    // Name in Hive 0.9 and lower
                    "org.apache.hadoop.hive.metastore.api.Constants");
            }

            // Make a new object for this class, using the default constructor
            constsFromReflection = constsClass.newInstance(); 
        } catch (InstantiationException e) { 
            throw new MetaException("Instantiation error for metastore constants class");
        } catch (IllegalAccessException e) { 
            throw new MetaException("Illegal access exception");
        } catch (ClassNotFoundException e) { 
            throw new MetaException("Could not find Hive Metastore constants class");
        } 
        // Using Java reflection, get a reference to the DDL_TIME field
       try {
            ddlTimeField = constsClass.getField("DDL_TIME");
        } catch (NoSuchFieldException e) {
            throw new MetaException("Could not find DDL_TIME constant field");
        }
        // get the String object that represents the value of this field
      try {
            fieldVal = ddlTimeField.get(constsFromReflection);

        } catch (IllegalAccessException e) {
            throw new MetaException("Could not get value for DDL_TIME constant field");
        }

        return fieldVal.toString();
  }

  public static void executeHiveSQL(String ddl) 
        throws ClassNotFoundException, SQLException
  {
      if (stmt == null) {
          Class.forName("org.apache.hive.jdbc.HiveDriver");
          Connection con = null;
          String isSecureHadoop = System.getenv("SECURE_HADOOP");
          //If Kerberos is enabled, then we need to connect to remote hiveserver2 using hive principal
          if(isSecureHadoop != null && isSecureHadoop.equalsIgnoreCase("Y")){
              String hiveServer2Url = System.getenv("HIVESERVER2_URL");
              if(hiveServer2Url == null || hiveServer2Url.isEmpty()){
                  hiveServer2Url = "localhost:10000";
              }
              String hiveServerAddress = "jdbc:hive2://" + hiveServer2Url + "/";
              String hivePrincipal = System.getenv("HIVE_PRINCIPAL");
              if(hivePrincipal != null && ! hivePrincipal.isEmpty()){
                 hiveServerAddress = hiveServerAddress + ";principal=" + hivePrincipal;
              }
              String hiveServerSsl = System.getenv("HIVESERVER2_SSL");
              if(hiveServerSsl != null && ! hiveServerSsl.isEmpty()){
                 hiveServerAddress = hiveServerAddress + ";ssl=" + hiveServerSsl;
              }
              con = DriverManager.getConnection(hiveServerAddress, "hive", "");
          }else{
              con = DriverManager.getConnection("jdbc:hive2://", "hive", "");
          }
          stmt = con.createStatement();
      }

      try {

          stmt.execute(ddl);
      } catch (SQLException s) {
          throw s;            
      }
  }

  // Create a partition for an existing Hive table
  //  schemaName:      Name of Hive database or default schema "default"
  //  tableName:       Name of Hive table
  //  partitionString: Partition column values as in the directory name,
  //                   e.g. "p1=1,p2=two"
  //  useDistributedLock: Coordinate with other executor processes to avoid
  //                      creating the same partition multiple times, see
  //                      Mantis 5092 and related cases Mantis-5282
  //                      and Mantis-5285 as well as HIVE-17249.
  public void createHiveTablePartition(String schemaName,
                                       String tableName,
                                       String partitionString,
                                       boolean useDistributedLock) throws IOException, KeeperException
  {
    if (logger.isDebugEnabled()) logger.debug("HiveClient.createHiveTablePartition(" + schemaName
                                              + ", " + tableName
                                              + ", " + partitionString + ") - started" );
    String lockName = "hv_" + tableName + partitionString;
    //remove the slash in the path
    String tmp = lockName.replace("/","_");
    String nln = tmp.replace(".", "_");
    DistributedLock lock = null;
    if (useDistributedLock)
       lock = new DistributedLock(nln);  
    try
    {
      //get lock first
      if (logger.isDebugEnabled()) logger.debug("HiveClient.createHiveTablePartition() -- try to lock");
      if (useDistributedLock)
      {
        if (lock.lock(10000 * 60) == -1) //timeout 10 min 
        {
          throw new IOException("HiveClient.createHiveTablePartition() -- lock fail due to timeout");
        }      
      }
      getHiveMetaClient().appendPartition(schemaName, tableName, partitionString);
    }
    catch (AlreadyExistsException ae)
    {
      if (logger.isDebugEnabled()) logger.debug("HiveClient.createHiveTablePartition() -- already exists");
      // do nothing, partition is already created
    }
    catch (Exception e)
    {
      if (logger.isDebugEnabled()) logger.debug("HiveClient.createHiveTablePartition() -- exception: ", e);
         throw new IOException(e);
    }
    finally {
      if (useDistributedLock)
        lock.unlock();
    }
    if (logger.isDebugEnabled()) logger.debug("HiveClient.createHiveTablePartition() - success");
    
  }

  
  // Find the Sentry privileges for the Hive table given by tableName for the
  // groups given by the groupNames array. Flatten that information into an
  // array of bitmaps and return it.
  //
  // The structure of the returned bitmap is as follows:
  //
  // 0 - schema privilege bitmap
  // 1 - object privilege bitmap
  // 2 through n+1, where n = # columns - column privilege bitmap
  // n+2 - schema privilege with grant option bitmap
  // n+3 - object privilege with grant option bitmap
  // n+4 through 2n+3, where n = # columns - column privilege with grant option bitmap
  //
  // So, the number of elements in the returned array is 2n+4, where n = # columns.

  public int[] getSentryPrivileges(Object[] groupNames, Object[] desiredColumns, String tableName)
  throws Exception
  {
    if (logger.isDebugEnabled()) logger.debug("HiveClient.getSentryPrivileges(" + tableName
                                              + ") - started" );

    // pseudo-code:
    // convert groupNames to a List<String>
    // call Sentry method
    // create an object array and put the bitmaps into it
    // return that object array

    List<String> groupNamesList = new ArrayList<String>();
    for (int i = 0 ; i < groupNames.length; i++) {
      String groupName = (String)groupNames[i];
      groupNamesList.add(groupName);
    }

    // TODO: Is using getInstance() good enough? What if we have to supply a
    // config file? Would it be better to create a persistent SentryPolicyService
    // object as a member of this class and keep it around for reuse?

    SentryPolicyService sps = SentryPolicyService.getInstance();

    // TODO: remove commented debug code below (when we don't think we need it anymore)
    //EsgynDBPrivilege privBitMaps;
    //if (sps == null) {
    //  if (logger.isDebugEnabled()) logger.debug("SentryPolicyService.getInstance() returned null" );
    //  // stub code to compensate when Sentry is absent
    //  privBitMaps = new EsgynDBPrivilege();
    //  // add some code to set specific privileges (as created, the object has no privileges)
    //  privBitMaps.setObjectPrivBitmap(EsgynDBPrivilege.Privilege.SELECT.getPrivilege() + 
    //                                 EsgynDBPrivilege.Privilege.INSERT.getPrivilege());
    //  privBitMaps.setObjectPrivWithGrantBitmap(EsgynDBPrivilege.Privilege.SELECT.getPrivilege());
    //}
    //else {
    //  privBitMaps = sps.getPrivBitMap(groupNamesList, tableName);
    //}
    
    if (sps == null)
      if (logger.isDebugEnabled()) logger.debug("SentryPolicyService.getInstance() returned null" );
      
    EsgynDBPrivilege privBitMaps = sps.getPrivBitMap(groupNamesList, tableName);

    // TODO: If we keep both of the getSentryPrivileges methods in the long term,
    // the following logic (which is identical between them) could be factored out
    // into a common private method.
     
    HashMap<String, Integer> colPrivBitmaps = privBitMaps.getColPrivBitmaps();
    HashMap<String, Integer> colPrivWGOBitmaps = privBitMaps.getColPrivWithGrantBitmaps();
    int numberOfColumns = desiredColumns.length;
    int[] results = new int[2*numberOfColumns+4];

    results[0] = privBitMaps.getSchemaPrivBitmap();
    results[1] = privBitMaps.getObjectPrivBitmap();

    for (int j = 0; j < numberOfColumns; j++) {
      Integer colPrivs = colPrivBitmaps.get(desiredColumns[j]); // null if name isn't in colPrivBitmaps
      if (colPrivs == null)
        results[j+2] = 0;
      else
        results[j+2] = colPrivs;
    } 

    results[numberOfColumns+2] = privBitMaps.getSchemaPrivWithGrantBitmap();
    results[numberOfColumns+3] = privBitMaps.getObjectPrivWithGrantBitmap();

    for (int k = 0; k < numberOfColumns; k++) {
      Integer colWGOPrivs = colPrivWGOBitmaps.get(desiredColumns[k]); // null if name isn't present
      if (colWGOPrivs == null)
        results[k+numberOfColumns+4] = 0;
      else
        results[k+numberOfColumns+4] = colWGOPrivs;
    } 
    
    return results;                                       
  }



  // Find the Sentry privileges for the Hive table given by tableName for the
  // user given by userName. Flatten that information into an array of bitmaps
  // and return it.
  //
  // The structure of the returned bitmap is as follows:
  //
  // 0 - schema privilege bitmap
  // 1 - object privilege bitmap
  // 2 through n+1, where n = # columns - column privilege bitmap
  // n+2 - schema privilege with grant option bitmap
  // n+3 - object privilege with grant option bitmap
  // n+4 through 2n+3, where n = # columns - column privilege with grant option bitmap
  //
  // So, the number of elements in the returned array is 2n+4, where n = # columns.

  public int[] getSentryPrivilegesByUserName(String userNameIn, Object[] desiredColumns, String tableName)
  throws Exception
  {
    String userName = userNameIn.toLowerCase();
    if (logger.isDebugEnabled()) logger.debug("HiveClient.getSentryPrivilegesByUserName(name in: " + userNameIn + ", name updated: " + userName + ", " + tableName
                                              + ") - started" );

    // pseudo-code:
    // call Sentry method
    // create an object array and put the bitmaps into it
    // return that object array

    // TODO: Is using getInstance() good enough? What if we have to supply a
    // config file? Would it be better to create a persistent SentryPolicyService
    // object as a member of this class and keep it around for reuse?

    SentryPolicyService sps = SentryPolicyService.getInstance();

    // TODO: remove commented debug code below (when we don't think we need it anymore)
    //EsgynDBPrivilege privBitMaps;
    //if (sps == null) {
    //  if (logger.isDebugEnabled()) logger.debug("SentryPolicyService.getInstance() returned null" );
    //  // stub code to compensate when Sentry is absent
    //  privBitMaps = new EsgynDBPrivilege();
    //  // add some code to set specific privileges (as created, the object has no privileges)
    //  privBitMaps.setObjectPrivBitmap(EsgynDBPrivilege.Privilege.SELECT.getPrivilege() + 
    //                                 EsgynDBPrivilege.Privilege.INSERT.getPrivilege());
    //  privBitMaps.setObjectPrivWithGrantBitmap(EsgynDBPrivilege.Privilege.SELECT.getPrivilege());
    //}
    //else {
    //  privBitMaps = sps.getPrivBitMap(groupNamesList, tableName);
    //}
    
    if (sps == null)
      if (logger.isDebugEnabled()) logger.debug("SentryPolicyService.getInstance() returned null" );
      
    EsgynDBPrivilege privBitMaps = sps.getPrivBitMapByUserName(userName, tableName);

    // TODO: If we keep both of the getSentryPrivileges methods in the long term,
    // the following logic (which is identical between them) could be factored out
    // into a common private method.
     
    HashMap<String, Integer> colPrivBitmaps = privBitMaps.getColPrivBitmaps();
    HashMap<String, Integer> colPrivWGOBitmaps = privBitMaps.getColPrivWithGrantBitmaps();
    int numberOfColumns = desiredColumns.length;
    int[] results = new int[2*numberOfColumns+4];

    results[0] = privBitMaps.getSchemaPrivBitmap();
    results[1] = privBitMaps.getObjectPrivBitmap();

    for (int j = 0; j < numberOfColumns; j++) {
      Integer colPrivs = colPrivBitmaps.get(desiredColumns[j]); // null if name isn't in colPrivBitmaps
      if (colPrivs == null)
        results[j+2] = 0;
      else
        results[j+2] = colPrivs;
    } 

    results[numberOfColumns+2] = privBitMaps.getSchemaPrivWithGrantBitmap();
    results[numberOfColumns+3] = privBitMaps.getObjectPrivWithGrantBitmap();

    for (int k = 0; k < numberOfColumns; k++) {
      Integer colWGOPrivs = colPrivWGOBitmaps.get(desiredColumns[k]); // null if name isn't present
      if (colWGOPrivs == null)
        results[k+numberOfColumns+4] = 0;
      else
        results[k+numberOfColumns+4] = colWGOPrivs;
    } 
    
    return results;                                       
  }

  public boolean getHiveTableInfo(long jniObject, String schName, String tblName, boolean readPartn)
       throws MetaException, TException
  {
     Table table;
     try {
        table = getHiveMetaClient().getTable(schName, tblName);
     } catch (NoSuchObjectException x) {
         return false; 
     } 
     String[] tableInfo = new String[Table_FIELD_COUNT];
     tableInfo[Table_TABLE_NAME] = table.getTableName();
     tableInfo[Table_DB_NAME]= table.getDbName();
     tableInfo[Table_OWNER] = table.getOwner();
     tableInfo[Table_CREATE_TIME] = Integer.toString(table.getCreateTime());
     tableInfo[Table_TABLE_TYPE] = table.getTableType();
     tableInfo[Table_VIEW_ORIGINAL_TEXT] = table.getViewOriginalText();
     tableInfo[Table_VIEW_EXPANDED_TEXT] = table.getViewExpandedText();

     StorageDescriptor sd = table.getSd();
     tableInfo[Table_SD_COMPRESSED] = Boolean.toString(sd.isCompressed());
     tableInfo[Table_SD_LOCATION] = sd.getLocation();
     tableInfo[Table_SD_INPUT_FORMAT] = sd.getInputFormat();
     tableInfo[Table_SD_OUTPUT_FORMAT] = sd.getOutputFormat();
     tableInfo[Table_SD_NUM_BUCKETS] = Integer.toString(sd.getNumBuckets());

     SerDeInfo serDe = sd.getSerdeInfo(); 
     Map<String,String> serDeParams = serDe.getParameters();
     tableInfo[Table_NULL_FORMAT] = serDeParams.get("serialization.null.format");
     tableInfo[Table_FIELD_DELIM] = serDeParams.get("field.delim");
     tableInfo[Table_LINE_DELIM] = serDeParams.get("line.delim");
     tableInfo[Table_SERIALIZE_LIB] = serDe.getSerializationLib();

     // Columns in the table
     int numCols = sd.getColsSize();
     String[][] colInfo = new String[numCols][Col_FIELD_COUNT];
     Iterator<FieldSchema> fieldIterator = sd.getColsIterator();
     int i = 0;
     FieldSchema field;
     while (fieldIterator.hasNext()) {
        field = fieldIterator.next();
        colInfo[i][Col_NAME] = field.getName(); 
        colInfo[i][Col_TYPE] = field.getType(); 
        i++;
     }
     String[][] partKeyInfo = null;
     String[][] partKeyValues = null;
     String[] partNames = null;
     String[] bucketCols = null;
     String[] sortCols = null;
     int[] sortColsOrder = null;
     String[] paramsKey = null;
     String[] paramsValue = null;
     int numPartKeys = table.getPartitionKeysSize();
     if (numPartKeys > 0) {
        partKeyInfo = new String[numPartKeys][Col_FIELD_COUNT];
        Iterator<FieldSchema> partKeyIterator = table.getPartitionKeysIterator();
        i = 0;
        FieldSchema partKey;
        while (partKeyIterator.hasNext()) {
           partKey = partKeyIterator.next();
           partKeyInfo[i][Col_NAME] = partKey.getName(); 
           partKeyInfo[i][Col_TYPE] = partKey.getType(); 
           i++;
        }
        if (readPartn) {
           List<String> partNamesList = getHiveMetaClient().listPartitionNames(schName, tblName, (short)-1);
           if (partNamesList != null) {
              partNames = new String[partNamesList.size()];
              partNames = partNamesList.toArray(partNames); 
              i = 0;
              partKeyValues = new String[partNames.length][];
              for (i = 0; i < partNames.length; i++) {
                  partKeyValues[i] = new String[numPartKeys];
                  partKeyValues[i] = (String[])getHiveMetaClient().partitionNameToVals(partNames[i]).toArray(partKeyValues[i]); 
              }
           }
        } 
     }
     
     // Bucket Columns
     int numBucketCols = sd.getBucketColsSize();
     if (numBucketCols > 0) {
        bucketCols = new String[numBucketCols];
        Iterator<String> bucketColsIterator = sd.getBucketColsIterator();
        i = 0;
        while (bucketColsIterator.hasNext()) {
           bucketCols[i++] = bucketColsIterator.next();
        }
     }
    
     // Sort Columns
     int numSortCols = sd.getSortColsSize();
     if (numSortCols > 0) {
        sortCols = new String[numSortCols];
        sortColsOrder = new int[numSortCols];
        Iterator<Order> sortColsIterator = sd.getSortColsIterator();
        i = 0;
        Order sortOrder;
        while (sortColsIterator.hasNext()) {
           sortOrder = sortColsIterator.next();
           sortCols[i] = sortOrder.getCol();
           sortColsOrder[i] = sortOrder.getOrder(); 
           i++;
        } 
     }

     // Table Params
     Map<String, String> unsortedParams = table.getParameters();
     Map<String, String> params = new TreeMap<String, String>(unsortedParams);
     paramsKey = new String[params.size()];
     paramsValue = new String[params.size()]; 
     i = 0;
     for (Map.Entry<String,String> entry : params.entrySet()) {
          paramsKey[i] = entry.getKey();
          paramsValue[i] = entry.getValue();
          i++;
     }
     // Replace creation time with redefineTime 
     String rfTime = params.get(ddlTimeConst);
     if (rfTime != null)
        tableInfo[Table_CREATE_TIME] = rfTime;
     if (tableInfo[Table_NULL_FORMAT] == null) {
        String nullFormat = params.get("serialization.null.format");
        if (nullFormat != null) 
           tableInfo[Table_NULL_FORMAT] = nullFormat;
     }
     setTableInfo(jniObject, tableInfo, colInfo, partKeyInfo, bucketCols, sortCols, sortColsOrder, paramsKey, paramsValue, partNames, partKeyValues);     
     return true;
  }

  class StripeOrRowgroupReadInfo implements Callable
  {
     StripeOrRowgroupReadInfo(long jniObj, int fileType, int lPartNum, String partName, int numFilesToSample, int numTasks, LocatedFileStatus fileStatus) 
     {
        m_fileType = fileType;
        m_jniobj = jniObj;
        m_fileStatus = fileStatus;
        m_partNum = lPartNum;
        if (partName != null)
           m_partKeyValues = partName.replaceAll("%3A",":");
        m_numFilesToSample = numFilesToSample;
        m_numTasksSubmitted  = numTasks;
     }

     public Object call() throws IOException, MetaException, TException
     {
        if (m_fileType == ORC)
           getOrcStripeInfo();
        else if (m_fileType == PARQUET)
	   getParquetRowgroupInfo();
        else
           throw new IOException("Unsupported file type " + m_fileType);
        BlockLocation[] locations = m_fileStatus.getBlockLocations();
        m_numBlocks = locations.length;
        short rf = m_fileStatus.getReplication();
        m_hosts = new String[m_numBlocks * rf];
        int blocksHostNum = 0;
        for (int blockNum = 0 ; blockNum < m_numBlocks; blockNum++) {
           String[] blockHosts = locations[blockNum].getHosts();
           blocksHostNum = blockNum * rf;
           for (int hostNum = 0; hostNum < blockHosts.length ; hostNum++) {
              m_hosts[blocksHostNum++] = blockHosts[hostNum];
           }
        }
        locations = null;
        Path parentDir = m_fileStatus.getPath().getParent();
        m_partDir = parentDir.toString();
        return this;
     }
   
     public void getParquetRowgroupInfo() throws IOException, MetaException, TException
     {
        if (m_numFilesToSample > 0 && m_numTasksSubmitted <= m_numFilesToSample) {
           ParquetFileReader reader = ParquetFileReader.open(hiveConf, m_fileStatus.getPath());
           List<BlockMetaData> bmds = reader.getRowGroups(); 
           m_numStripesOrRowgroups = bmds.size(); 
           m_offsets = new long[m_numStripesOrRowgroups];
           m_lengths = new long[m_numStripesOrRowgroups];
           m_numRows = new long[m_numStripesOrRowgroups];
           Iterator<BlockMetaData> bmdItor = bmds.iterator();
           for (int j = 0; j < m_numStripesOrRowgroups ; j++) {
              BlockMetaData bmd = bmdItor.next();
              m_offsets[j] = bmd.getStartingPos();
              m_lengths[j] = bmd.getTotalByteSize();
              m_numRows[j] = bmd.getRowCount();
           }
           reader.close();
           reader = null;
        }
        return;
     }

     public void getOrcStripeInfo() throws IOException, MetaException, TException
     {
        if (m_numFilesToSample > 0 && m_numTasksSubmitted <= m_numFilesToSample) {
           Reader reader = OrcFile.createReader(m_fileStatus.getPath(), OrcFile.readerOptions(hiveConf));
           List<StripeInformation> stripes = reader.getStripes();
           m_numStripesOrRowgroups = stripes.size(); 
           m_offsets = new long[m_numStripesOrRowgroups];
           m_lengths = new long[m_numStripesOrRowgroups];
           m_numRows = new long[m_numStripesOrRowgroups];
           Iterator<StripeInformation> siItor = stripes.iterator();
           for (int j = 0; j < m_numStripesOrRowgroups ; j++) {
              StripeInformation si = siItor.next();
              m_offsets[j] = si.getOffset();
              m_lengths[j] = si.getIndexLength() + si.getDataLength() + si.getFooterLength();
              m_numRows[j] = si.getNumberOfRows();
           }
           reader = null;
        }
        return;
     }
     
     long m_jniobj; 
     int m_fileType;
     int m_partNum;
     String m_partName; 
     LocatedFileStatus m_fileStatus; 
     int m_numStripesOrRowgroups;
     long[] m_offsets;
     long[] m_lengths;
     long[] m_numRows;
     int m_numBlocks; 
     String[] m_hosts;
     String m_partDir;
     String m_partKeyValues;
     int m_numFilesToSample;
     int m_numTasksSubmitted;
  }

  // From parquet-hadoop/src/main/java/org/apache/parquet/hadoop/util/HiddenFileFilter.java
  // to ensure compatibility on which files are skipped by Parquet reader.
  // We will use same convention for ORC files.
  // For directories we will only skip those starting with ".", till we 
  // decide if a directory that starts with "_division_=1" is acceptable 
  // Hive2/TEST057
  public boolean accept(Path p){
      String name = p.getName(); 
      return !name.startsWith("_") && !name.startsWith("."); 
  }

  // This function recursively traverses the directories starting from the table directory.
  // The subdirectory level denotes the partition key value and the name of the subdirectory will be of format partKeyName=partKeyVal
  // The subdirectory name is concatenated as partName
  // The parameter numPartKeys denotes the depth to which this function needs to traverse.
  // If the table is not partitioned, then this function just looks at the files in the given directory

  private int submitTasks(long jniObj, int fileType, ExecutorCompletionService<StripeOrRowgroupReadInfo> ecs, FileSystem fs, Path dirPath, int numPartKeys, 
            int numFilesToSample, boolean readSubdirs, int depth, String partName, long[] latestDirTimestamp) throws IOException
  {
     int numOfTasks = 0;
     boolean partNumIncr = false;
     RemoteIterator<LocatedFileStatus> fileStatusItor = null;
     fileStatusItor = fs.listLocatedStatus(dirPath);  
     LocatedFileStatus aFileStatus;
     while (fileStatusItor.hasNext()) {
        aFileStatus = fileStatusItor.next();
        if (aFileStatus.isDirectory()) {
	    if (aFileStatus.getPath().getName().startsWith("."))
		continue;
           long dirTimestamp = aFileStatus.getModificationTime();
           if (dirTimestamp > latestDirTimestamp[0])
              latestDirTimestamp[0] = dirTimestamp;
           if (depth < numPartKeys) {
              String tmp;
              tmp = aFileStatus.getPath().getName(); 
              tmp = tmp.substring(tmp.indexOf('=')+1);
              if (partName == null)
                 numOfTasks += submitTasks(jniObj, fileType, ecs, fs, aFileStatus.getPath(), numPartKeys, numFilesToSample, readSubdirs, depth+1, 
                      tmp, latestDirTimestamp); 
              else
                 numOfTasks += submitTasks(jniObj, fileType, ecs, fs, aFileStatus.getPath(), numPartKeys, numFilesToSample, readSubdirs, depth+1, 
                      partName + ',' + tmp, latestDirTimestamp); 
           }
           else
              if (readSubdirs) 
                 numOfTasks += submitTasks(jniObj, fileType, ecs, fs, aFileStatus.getPath(), numPartKeys, numFilesToSample, readSubdirs, depth+1, 
                      partName, latestDirTimestamp); 
              else
                 throw new IOException("Unexpected directory " + aFileStatus.getPath().toString());
        } else {
	    if (!accept(aFileStatus.getPath()))
		continue;
            if (!readSubdirs) {
               if (depth != numPartKeys)
                   throw new IOException("Unexpected file " + aFileStatus.getPath().toString());
            }
            if ((numPartKeys != 0) && (!partNumIncr)) {
               partNum++;
               partNumIncr = true;
            }
            numOfTasks++; 
            StripeOrRowgroupReadInfo stripeOrRowgroupReadInfo = new StripeOrRowgroupReadInfo(jniObj, fileType ,partNum, partName, numFilesToSample, 
                                 numOfTasks, aFileStatus);
            ecs.submit(stripeOrRowgroupReadInfo);
        }
     }
     return numOfTasks;
  }

  // This function calls submitTasks to submit the tasks to Executors framework
  // Then it runs 'n' number of tasks in parallel. The number of threads to run in parallel
  // is controlled by the property traf.orc.num.stripeOrRowgroupinfo.threads in trafodion-site.xml
  // Default is 10 threads.
  // When a task complete, another submitted task will be taken up to run
  // The completed task sends the information about the file to the JNI layer
  // for populating as a hierachy of statistics information 
  // Hierarchy consists of the following
  // HHDFSTableStats-->HHDFSPartitionStats-->HHDFSBucketStats-->HHDFSFileStats
  // partition number 0 is used when the file is not paritioned.
  // bucket number 0 is used when the table is not bucketed. 
  
  public long getStripeOrRowgroupInfo(long jniObj, int fileType,  String pathStr, int numPartKeys, 
                               int numFilesToSample, boolean readSubdirs) throws IOException 
  {
     Path listPath = new Path(pathStr);
     FileSystem fs = FileSystem.get(listPath.toUri(), hiveConf);
     FileStatus dirStatus = fs.getFileStatus(listPath);
     StripeOrRowgroupReadInfo stripeOrRowgroupReadInfo = null;
     ExecutorCompletionService<StripeOrRowgroupReadInfo>  ecs = new ExecutorCompletionService(getExecutorService());
     int numOfTasks = 0;
     int partNum = 0;
     String partName = null;
     int depth = 0;   
     long latestDirTimestamp[] = new long[1];
     latestDirTimestamp[0] = dirStatus.getModificationTime();
     numOfTasks = submitTasks(jniObj, fileType, ecs, fs, listPath, numPartKeys, numFilesToSample, readSubdirs, depth, partName, latestDirTimestamp); 
     for (int i = 0; i < numOfTasks; i++) {
        boolean loopExit = false;
        do
        {
           try {
              stripeOrRowgroupReadInfo = ecs.take().get();
              loopExit = true;
           }
           catch (InterruptedException ie) {}
           catch (ExecutionException e) {
              throw new IOException(e);
           }
        } while (loopExit == false);
        Path path = stripeOrRowgroupReadInfo.m_fileStatus.getPath();
        setStripeOrRowgroupInfo(stripeOrRowgroupReadInfo.m_jniobj, 
                stripeOrRowgroupReadInfo.m_partNum,
                stripeOrRowgroupReadInfo.m_fileStatus.getPath().toString(),
                stripeOrRowgroupReadInfo.m_partDir, 
                stripeOrRowgroupReadInfo.m_partKeyValues,
                stripeOrRowgroupReadInfo.m_fileStatus.isFile(),
                stripeOrRowgroupReadInfo.m_fileStatus.getModificationTime(),
                stripeOrRowgroupReadInfo.m_fileStatus.getLen(),  stripeOrRowgroupReadInfo.m_fileStatus.getReplication(),
                stripeOrRowgroupReadInfo.m_fileStatus.getBlockSize(),  stripeOrRowgroupReadInfo.m_fileStatus.getAccessTime(),
                stripeOrRowgroupReadInfo.m_numStripesOrRowgroups, stripeOrRowgroupReadInfo.m_offsets, 
                stripeOrRowgroupReadInfo.m_lengths, stripeOrRowgroupReadInfo.m_numRows, 
                stripeOrRowgroupReadInfo.m_numBlocks, stripeOrRowgroupReadInfo.m_hosts); 
    } 
    return latestDirTimestamp[0];
  }

  private native void setStripeOrRowgroupInfo(long jniObject, int partNum, String fileName, String partDir, String partKeyValues, 
             boolean isFile,
             long modificationTs, long fileSize, short replicationFactor, long blockSize, long accessTs, 
             int numStripesOrRowgroups, long[] offsets, long[] lengths, long[] numRows, int numBlocks, String[] hosts);
  private native void setTableInfo(long jniObject, String[] tableInfo, String[][] colInfo, String[][] partKeyInfo,
                         String[] bucketCols, String[] sortCols, int[] sortColsOrder, String[] paramsKey, String[] paramsValue,
                         String[] partNames, String[][] partKeyValues);
}
