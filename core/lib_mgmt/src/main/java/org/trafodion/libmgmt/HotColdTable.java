// @@@ START COPYRIGHT @@@
//
// (C) Copyright 2016-2018 Esgyn Corporation
//
// @@@ END COPYRIGHT @@@
package org.trafodion.libmgmt;

import org.apache.log4j.Logger;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Writer;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.TreeMap;
import java.util.LinkedList;
import java.util.NoSuchElementException;
import java.util.UUID;

public class HotColdTable  {
	static Logger logger = Logger.getLogger(HotColdTable.class.getName());
    private static final int COM_HOTCOLD_TEXT = 11; // must match ComSmallDefs.h ComTextType.COM_HOTCOLD_TEXT
    private static final int CUTOFF_SCALAR_STATEMENT = 0;
    private static final int COLUMN_LIST = 1;
    private static final int CUTOFF_CURRENT_VALUE = 2;
    private static final int COLUMN_LIST_FOR_UPSERT = 3;
    private static final int SEMAPHORE_FOR_CRON_JOB = 4;
    
    private static final String ERROR_TABLE_NOT_FOUND = "38001";
    private static final String ERROR_TRAFODION_DRIVER_NOT_FOUND = "38002";
    private static final String ERROR_WRONG_PARAMETER = "38003";
    private static final String ERROR_DB_CONNECTION_FAILED = "38004";
    private static final String ERROR_UNSUPPORTED_TYPE = "38005";
    private static final String ERROR_ROLLBACK_FAILED = "38006";
    private static final String ERROR_CREATE_FAILED_AND_ROLLEDBACK ="38007";
    private static final String ERROR_MOVETOCOLD_FAILED_AND_ROLLEDBACK ="38008";
    private static final String ERROR_CLEANUP_FAILED = "38009";
    private static final String ERROR_UNSUPPORTED_SCHEMA_TABLE_COLUMN_NAME = "38010";    

    
    //retrieve table metadata query
    private  static String retrieveTableInfo = 
        "select"+ 
        "   c.column_name,"+
        "   CASE WHEN c.column_name = idx.column_name THEN 'T' ELSE 'F' END as BLOOM_FILTER,"+
        "   k.keyseq_number,"+
        "   c.column_number,"+
        "   c.column_class,"+
        "   c.fs_data_type,"+
        "   c.sql_data_type,"+
        "   c.character_set,"+
        "   c.column_size,"+
        "   c.column_precision,"+
        "   c.column_scale "+
        "from \"_MD_\".objects o left join \"_MD_\".columns c "+ 
        "on o.object_uid=c.object_uid "+
        "left join \"_MD_\".keys k "+
        "on o.object_uid = k.object_uid and c.column_name = k.column_name "+
        "left join "+ 
        "( "+
        " select SUBSTRING(column_name,0,char_length(column_name)) column_name from "+
        "    ( "+
        "     select column_name,keyseq_number,min(keyseq_number) over (partition by object_uid) as min_keyseq_number  from "+ 
        "       ( "+
        "       select k.object_uid, k.column_name, k.keyseq_number from \"_MD_\".keys k join "+ 
        "         (select * from \"_MD_\".objects o left join \"_MD_\".indexes i on o.object_uid=i.base_table_uid "+
        "          where o.catalog_name = 'TRAFODION' and o.schema_name=? and o.object_name=? and o.object_type = 'BT' and i.is_explicit = 1 "+
        "         ) ind "+
        "       on k.object_uid = ind.index_uid "+
        "       where SUBSTRING(k.column_name,0,7) <> '_SALT_' and SUBSTRING(k.column_name,0,11) <> '_DIVISION_' "+ 
        "       order by k.object_uid, keyseq_number "+
        "       ) "+
        "    )where keyseq_number = min_keyseq_number "+
        ") idx on c.column_name = idx.column_name "+
        "where o.catalog_name = 'TRAFODION' and o.schema_name=? and o.object_name=? and o.object_type = 'BT' and c.column_name <> '_SALT_' and c.column_name <> 'SYSKEY'"+
        "order by k.keyseq_number, c.column_number; ";
    private static String upsertHotColdTextData = String.format("upsert into trafodion.\"_MD_\".text "+
        "select object_uid,%d,?,0,0,? from  trafodion.\"_MD_\".objects where "+ 
        "catalog_name = 'TRAFODION' and schema_name = ? and object_name = ? and object_type = 'BT';",COM_HOTCOLD_TEXT);

    private enum cutoffType {TIMESTAMP, DATE, CHAR, TINYINT, SMALLINT, INT, BIGINT}; // used to pick the right SQL syntax when comparing literals
    
    
    public static void main(String[] args) throws Exception{
        Connection conn = null;
        try{
          	  Class.forName("org.apache.trafodion.jdbc.t2.T2Driver");
        } catch (Exception e) {
            System.out.println(String.format("T2 driver not found"));
            System.exit(1);
            return;
        }			
        try {
          	  conn = DriverManager.getConnection( "jdbc:t2jdbc:" );
        } catch (SQLException e) {
        	System.out.println(String.format("T2 driver not found"));
            logger.error(String.format("Database connection failed"));
            System.exit(1);
            return;
        }
    	if (args.length == 0){
    		System.out.println("argument expected are: Schema_name Table_name DOP [sortColumns]");
    		conn.close();
    		System.exit(1);
    		return;
    	}
    	if ((args.length != 3) && (args.length != 4)){
    		logger.error("invalid number of arguments in HotColdTable main. 3 or 4 arguments expected");
    		System.out.println("invalid number of arguments in HotColdTable main. 3 or 4 arguments expected");
    		conn.close();
    		System.exit(1);
    		return;
    	}
      try{
    	  MoveFromHotToColdPrivate(args[0],args[1], Short.parseShort(args[2]), args.length == 3 ? null : args[3],(short)0, false, false,conn);
    	  conn.close();
    	  System.exit(0);
      }catch(Exception e){
    	  System.out.println("e");
    		logger.error(String.format("Exception in HotColdTable.main(): %s", e.getMessage()));
    		System.err.println(String.format("Exception in HotColdTable.main(): %s", e.getMessage()));
    		if (conn != null) conn.close();
    		System.exit(1);
      }
    }
    
    //default value for the simple API is DOP 2, so one file per partition, and no specific sort column, and cronJob running at 4am
    public static void CreateHotColdTable(String primarySchema, 
                                          String primaryTable, 
                                          String cutoffSQLScalarStatement) throws SQLException{
        CreateHotColdTableAdvanced(primarySchema, primaryTable, cutoffSQLScalarStatement,"ORC",(short)2,null,(short)4);
    }
	
    //procedure used to transform a regular trafodion table into a hot/cold table.
    //input: primarySchema -> schema name of input table (note we only support TRAFODION catalog)
    //       primaryTable -> input table name
    //       cutoffSQLScalarStatement -> SQL statement including ether CURRENT_TIMESTAMP or CURRENT_DATE that would return the
    //                                   cutoff date or time or int or whatever the type of partition column is.
    //                                   exemple: 'date_trunc(''MONTH'', CURRENT_TIMESTAMP) - INTERVAL ''2'' MONTH'
    //                                            'date_trunc(''DAY'', CURRENT_TIMESTAMP) - INTERVAL ''2'' DAY'
    //                                            'datediff(DAY, DATE ''1900-01-02'', CURRENT_DATE) + 2415022 - 2' ... this one is for the famous TPCDS STORE_SALES table
    //       coldTableFormat -> "ORC" or "PARQUET"
    //       dop -> this will tell how many files per partition we will create. ideally 1. But for some use case you can want more to increase the potential parallelism 
    //              on one partition. It is equivalent to bucketization in hive. So if you want to buckatize partitions, this is the way to do it. It will buckatize
    //              on _SALT_ column, so it should be inherantly perfect from a load balance stanpoint if the trafodion table was well balanced too (Correct _SALT)).
    //              Note that in production we can have a problem initial loading with DOP 1: in order to load data in parallel, with DOP 1, we have to use Type 4
    //              driver. Collecting user/password for type 4 driver uses a trick looking at trafci install and conveniance script that makes trafci from cluster not
    //              ask for user/password. However, on production, this feature might not work, and you may have to manually alter the trafci conveniance script with the
    //              real user name/ password of trafodion account. Else the DOP 1 will fall back on DOP 2, that uses Type2 driver to perform the job.
    //              Also note that with DOP1, the actual number of threads we will use to perform the loading in parallel is going to be ideally equals to the number of 
    //              partitions of the trafodion table, but if there is not enough MXO server to handle that many connections, we will automatically fall back to the number
    //              of MXO server configured on the cluster, minus 1 MXO server per node that we keep for other task to be performed.
    //       sortColumns -> ORC predicate push down can be very efficient on sorted columns. By default, with no sort columns, the data will be naturally sorted on the
    //                      second left most column of the trafodion table key. Most likelly, if the trafodion table was correctly designed, that is what you want to happen.
    //                      However, if you think that another sort columns would be more appropriate to sort on, you can specify them here. basically we will append
    //                      the following to the select statement feeding the upsert: String.format("order by %s", sortColumns))
    //       cronjobHour -> define at what hour of the day cronjob for hotcold move should run. from 0 to 23, any other value can be used to set the cronjob to run every hour.
    // processing:
    // ===========
    // 1/ collect trafodion table metadata
    // 2/ try to build a partitionned ORC table based on the trafodion table
    //      - partition column will be left most key column of trafodion table (excluding _SALT_)
    //      - if an index existed on the base table, a bloom filter will be created on the column it targetted (useless for 2.3 as bloom filters are not supported)
    //      - if a column type has no equivalent in HIVE types, abort with failure.
    //    if a partitionned ORC table already existed, keep it and use it for the rest of the processing: this allow manual tuning of the COLD table DDL if user
    //    wants to tweak some tblproperties values.
    // 3/ create hive schema with same schema name as the trafodion input schema if it did not exist already.
    // 4/ create the cold table if it did not exist in the created hive schema.
    // 5/ store several things in metadata
    //      - the cutoffSQLScalarStatement statement.
    //      - the culumn list for the upsert statement (needed because ORC imposes the partition column to be last in DDL statement, so columns canot have same order
    //        between hot and cold
    //      - the column list for the views (for the same reason as above)
    // 6/ invoke moveFromHotToCold, to move data from hot to cold, with the parametter stating that this is an invoke from CreateHotCold so that the moveToCold can not only
    //    perform the copy of data from hot to cold based on cutoff date, and create or update the views with new cutoff date, but also install the cron jobs on all 
    //    nodes (in fact up to 29 nodes) that is supposed to run ether daily or hourly to check if data should be copied from hot to cold.
    // note that we do not delete data from hot, ever. It is up to outside manually create process to perform deletion og data in hot, like any other table where users
    // have to manage archiving and deletions. As good practive, TTL feature of Hbase table could be used for that purpose, if the base table does not have any index
    // or delete statement must be used in case of base tables coupled with associated indexes.
    // refer to comments on coments on MoveFromHotToCold for more details on what is happening inside.
    // Given that ORC DDL are not transactional, we are managing rollback on failure manually, using a state variable keeping track of where we failed, so that proper rollback
    // can be performed.
    public static void CreateHotColdTableAdvanced(String primarySchemaIn,
                                                  String primaryTableIn,
                                                  String cutoffSQLScalarStatement,
                                                  String coldTableFormatIn,
                                                  short dop,
                                                  String sortColumns,
                                                  short cronjobHour
                                                 ) throws SQLException{
    //validate parameters
    if (primarySchemaIn == null || primarySchemaIn.length() == 0 
            || primarySchemaIn.contains("\"") 
            || primarySchemaIn.contains(".") 
            || primarySchemaIn.contains(","))
            throw new SQLException("Invalid schema name: must be a valid schema name (\",. and , are not supported for hotcold schemas)", ERROR_WRONG_PARAMETER);
    if (primaryTableIn == null || primaryTableIn.length() == 0 
            || primaryTableIn.contains("\"")
            || primaryTableIn.contains(".")
            ||primaryTableIn.contains(","))
            throw new SQLException("Invalid table name: must be a valid table name (\",. and , are not supported for hotcold tables)", ERROR_WRONG_PARAMETER);    	
    if (coldTableFormatIn == null || !(coldTableFormatIn.equalsIgnoreCase("ORC") || coldTableFormatIn.equalsIgnoreCase("PARQUET")))
            throw new SQLException("Invalid parameter coldTableFormat: must be ether ORC or PARQUET", ERROR_WRONG_PARAMETER);
	String primarySchema = 	primarySchemaIn.toUpperCase();
	String primaryTable = primaryTableIn.toUpperCase();
	String coldTableFormat = coldTableFormatIn.toUpperCase();
    
    try{//global try catch for ease of degub/troubleshoot. (stack trace print of unexpected exceptions)
        Connection conn = null;
        //Initialize DB connection
        
        //debug begin
        /*
            Properties props = new Properties();
                props.setProperty("catalog","trafodion");
                props.setProperty("user", "db__root");
                props.setProperty("password", "dummy");
            
            try{
                 Class.forName("org.trafodion.jdbc.t4.T4Driver");
            } catch (Exception e) {
                throw new SQLException("T4 driver not found", ERROR_TRAFODION_DRIVER_NOT_FOUND);						
            }			
            try {
                    conn = DriverManager.getConnection("jdbc:t4jdbc://localhost:23400/:", props);
            } catch (SQLException e) {
               // success=true; // if we cannot connect one thread that is OK. so keep success on. it may well be because we are running out of MXOSRVR
                //System.out.println(String.format("Thread %d Exception:%s",taskNb,e.toString()));
                throw new SQLException("Database connection failed", ERROR_DB_CONNECTION_FAILED);						
            }
        */
        
        ///debug end
        try {
            conn = DriverManager.getConnection( "jdbc:default:connection" );
        } catch (SQLException e) {
            throw new SQLException("Database connection failed", ERROR_DB_CONNECTION_FAILED);						
        }
        int stateForRollback = 0; //  variable to track progress so we can rollback in case of exception
        boolean coldTableAlreadyExisted = false; // variable to track if we created a cold table, or if it was pre-created
        try {
            HashMap<String,String> columnNames = new HashMap<String,String>(); 
            StringBuilder hiveDDL = new StringBuilder(10000);
            StringBuilder partitionBy = new StringBuilder(1000);
            StringBuilder bloomFilterColumns = new StringBuilder(1000);
            TreeMap<Integer,String> columnListForView = new TreeMap<Integer,String>();
            StringBuilder columnListForUpsert = new StringBuilder(1000);
            hiveDDL.append("CREATE TABLE  `")
                  .append(primarySchema)
                  .append("`.`")
                  .append(primaryTable)
                  .append("`(\n");
            // retrieve table info from meta data table.
            PreparedStatement preparedStatement = conn.prepareStatement(retrieveTableInfo);
            preparedStatement.setString(1, primarySchema);
            preparedStatement.setString(2, primaryTable);
            preparedStatement.setString(3, primarySchema);
            preparedStatement.setString(4, primaryTable);
            ResultSet rs = preparedStatement.executeQuery();
            boolean isFirst = true;
            boolean isFirstBloomFilter = true;
            int partitioncolumnCount = 1; //in the future may be we want to support multi-column partition?
            String partitionColumnName = null;
            boolean isEmpty = true;
            while (rs.next()){
                isEmpty = false;
                // manage column list for view using treemap to reorder based on column_number
                if (!rs.getString("COLUMN_CLASS").equals("S ")){// skip system columns like SYSKEY, _DIVISION_ or _SALT_
                    columnListForView.put(rs.getInt("COLUMN_NUMBER"), rs.getString("COLUMN_NAME"));
                }                
                if (partitioncolumnCount> 0){//create partition column
                    if (partitioncolumnCount != 1)// deal with the , in case there is more than one partition column
                        partitionBy.append(',');
                    addColumnOrcTableDDL(partitionBy,rs,columnNames,coldTableFormat.equals("ORC"));
                    partitionColumnName = rs.getString("COLUMN_NAME");
                    partitioncolumnCount--;
                }else{
                    if (!isFirst) {
                        hiveDDL.append("\n,");
                        columnListForUpsert.append(',');
                    }
                    isFirst = false;
                    // deal with bloom filter
                    if (rs.getString("BLOOM_FILTER").equals("T")){
                        if (!isFirstBloomFilter)
                            bloomFilterColumns.append(",");
                        isFirstBloomFilter = false;
                        bloomFilterColumns.append('`')
                                          .append(rs.getString("COLUMN_NAME"))
                                          .append('`');
                    }
                    //add column to ddl
                    addColumnOrcTableDDL(hiveDDL,rs,columnNames,coldTableFormat.equals("ORC")); // this function perform matching between Trafodion and ORC types
                                                                 // it will throw exception if there is a column without corresponding ORC data type
                    columnListForUpsert.append('"').append(rs.getString("COLUMN_NAME")).append('"');
                }				
            }//end while			
            rs.close();
            preparedStatement.close();
            if (isEmpty)
            {
                //conn.close();
                throw new SQLException(String.format("Table %s.%s does not exist",primarySchema,primaryTable),ERROR_TABLE_NOT_FOUND);
            }
            hiveDDL.append(")\n")
                  .append("PARTITIONED BY(")
                  .append(partitionBy.toString())
                  .append(")\n");
            if (coldTableFormat.equals("ORC"))
                  hiveDDL.append("STORED AS ORC");
            else
                  hiveDDL.append("STORED AS PARQUET");
            columnListForUpsert.append(",\"").append(partitionColumnName).append('"');
            if (bloomFilterColumns.length() > 0 && coldTableFormat.equals("ORC")) // there is at least one bloom filter column
            {
                hiveDDL.append("\nTBLPROPERTIES(")
                      .append("\"orc.bloom.filter.columns\"=\"")
                      .append(bloomFilterColumns.toString())
                      .append("\")");
            }			
            System.out.println("CREATE SCHEMA IF NOT EXISTS `"+primarySchema+"`");
            System.out.println(hiveDDL.toString());
            // create schema if not exists in Hive
            Statement statement = conn.createStatement();
            stateForRollback = 1;
            statement.execute(String.format("process hive statement 'CREATE SCHEMA IF NOT EXISTS `%s`';",primarySchema));			
            // create ORC table if not exists
            stateForRollback = 2;
            try{
            statement.execute(String.format("process hive statement '%s';",hiveDDL.toString()));
            }catch(SQLException e){
                if (e.getErrorCode() == -1214 && 
                    e.toString().contains("from org.apache.hadoop.hive.ql.exec.DDLTask. AlreadyExistsException")
                )
                    coldTableAlreadyExisted = true; // tag pre-existing, so that we don't delete cold table on a rollback.
                else
                	throw (e);
            }
            //~~EO begin this is where adding the register command causes systematic core from assert about recursive compile 
            //statement.execute(String.format("register hive table hive.\"%s\".\"%s\";",primarySchema,primaryTable));
            //~~EO end
            // store scalar formula computing hot/cold cut-over
            stateForRollback = 3;
            statement.execute("set parserflags 131072;");//turn on magic word to allow writing in _MD_ normally write protected
            preparedStatement= conn.prepareStatement(upsertHotColdTextData);
            preparedStatement.setInt(1, CUTOFF_SCALAR_STATEMENT);
            preparedStatement.setString(2, cutoffSQLScalarStatement);			
            preparedStatement.setString(3, primarySchema);
            preparedStatement.setString(4, primaryTable);
            stateForRollback = 4;
            int i = preparedStatement.executeUpdate();
            if (i!=1) throw (new SQLException("Failed to write HotCold Cutoff in metadata")); // should never happen
            preparedStatement= conn.prepareStatement(upsertHotColdTextData);
            preparedStatement.setInt(1, COLUMN_LIST_FOR_UPSERT);
            preparedStatement.setString(2, columnListForUpsert.toString());			
            preparedStatement.setString(3, primarySchema);
            preparedStatement.setString(4, primaryTable);
            stateForRollback = 5;
            i = preparedStatement.executeUpdate();
            if (i!=1) throw (new SQLException("Failed to write HotCold columnListForUpsert in metadata")); // should never happen			

            Iterator<Entry<Integer, String>> iter = columnListForView.entrySet().iterator();
            StringBuilder columnListStrBld = new StringBuilder(1000);
            isFirst = true; // to manage comma in column list
            while(iter.hasNext()){
                Entry<Integer, String> entry = iter.next();
                if (!isFirst)columnListStrBld.append(','); 
                columnListStrBld.append('"').append(entry.getValue()).append('"');
                isFirst = false;
            }
            String columnListStr = columnListStrBld.toString();
            // store column list for view and upsert statement
            preparedStatement.setInt(1,COLUMN_LIST);
            preparedStatement.setString(2, columnListStr);			
            preparedStatement.setString(3, primarySchema);
            preparedStatement.setString(4, primaryTable);
            stateForRollback = 6;
            i = preparedStatement.executeUpdate();
            if (i!=1) throw (new SQLException("Failed to write columnList in metadata")); // should never happen
            stateForRollback = 7;
            statement.execute("reset parserflags 131072;");//turn off magic word to allow writing in _MD_
            preparedStatement.close();
            statement.close();
            stateForRollback = 8;
            //invoke MoveFromHotToCold
            // this will perform the rest of the work
            // 1/
            MoveFromHotToColdPrivate(primarySchema,primaryTable,dop,sortColumns,cronjobHour,true,
                                     bloomFilterColumns.length() > 0,//if the condition is true, we have at least one index. Disable poor man mdam optimization.
                                     conn);
        } catch (Exception e) {
            boolean successfullRollback = true;
            Statement s = null;
            try{
              s = conn.createStatement();
              if (stateForRollback >= 5){
                s.execute("set parserflags 131072;");//turn off magic word to allow writing in _MD_ normally write protected
                s.executeUpdate(String.format("delete from trafodion.\"_MD_\".text where text_uid = (select object_uid "+ 
                           "from trafodion.\"_MD_\".objects where "+
                           "catalog_name = 'TRAFODION' and schema_name = '%s' and object_name = '%s' and object_type = 'BT') and text_type=%d;",
                           primarySchema,primaryTable,COM_HOTCOLD_TEXT));
              }
              if (stateForRollback >= 4){
                s.execute("reset parserflags 131072;");//turn off magic word to allow writing in _MD_ normally write protected
              }
              if (stateForRollback >= 3){
                  if (!coldTableAlreadyExisted)
                      s.execute(String.format("process hive statement 'DROP TABLE IF EXISTS `%s`.`%s` PURGE';",primarySchema, primaryTable));
              }
              if (stateForRollback >= 2){
                try{
                  s.execute(String.format("process hive statement 'DROP SCHEMA IF EXISTS `%s` RESTRICT';",primarySchema));
                }catch(SQLException ee){};//ignore case when schema cannot be dropped because table still exists
              }
            s.close();
             }catch(Exception ex){
                successfullRollback = false;
                if (s != null) s.close();
                //if (conn!= null) conn.close();
          }
          if (successfullRollback)
              throw  new SQLException("HotCold CREATE operation failed, and successfully rolled back. Original exception:\n"+e.toString(),ERROR_CREATE_FAILED_AND_ROLLEDBACK);
          else throw new SQLException("Failed to rollback. please run CALL HotCold('CLEANUP',schemaname, tablename) to get back to a clean state.\n Original failure: "+
                                      e.toString());
        }	
      }catch(Exception e){
          StringWriter sw = new StringWriter();
          e.printStackTrace(new PrintWriter(sw));
          String exceptionAsString = sw.toString();
          logger.error(String.format("HotColdCreate %s.%s exception:\n%s",primarySchema,primaryTable,exceptionAsString));
          throw new SQLException(exceptionAsString);
      }		
    }


    // Advanced API (can specify DOP and sort column)
    public static void MoveFromHotToCold(String schema, String tableName, short dop, String sortColumns) throws SQLException, ParseException{
        MoveFromHotToColdPrivate(schema, tableName, dop, sortColumns,(short)0, false, false,null);
    }
   
    //get current cutoff, calculate what would be the cutoff once cuttoff formula is applied
    //verify if semaphore is not already grabbed by another run on another node to perform MovefromHotToCold
    //if not, grab the semaphore so we have exclusive duty to perform the MoveFromHotToCold
    //if current cutoff and new cutoff based on cutoffFormula mismatch, move cutoff to new value return by formula
    //  - copy hot data to partitions between old cutoff and new cutoff
    //  - if coming from hotColdCreate, perform the upsert in parallel to optimize runtime of initial hotcold creation
    //  - respect dop parametter, it dictates how many files per partition will be created
    //  - respect the sortColumn, it define an optional sort order column if the default sorting by primary key is not desired
    //create or update the UNION ALL view (and view2 if hot table is using divisioning: view2 is temporary workaround until we support
    //divisioning on HIVE tables).
    //  - store new hotcold cutover in metadata
    //  - if called from create, install cronjob that will periodically invoke MoveFromHotToCold on up to 29 nodes, 2 mn apparts to unsure HA
    //     - cronjobHour allow specifying at what hour of the day, the cronjob will kick in. valid values are from 0 to 23. Any other value 
    //       will make cronjob execute every hour on each node of the cluster.
    //manually rollback if any problem encountered in the function.
    @SuppressWarnings("resource")
    private static void MoveFromHotToColdPrivate(String schema, String tableName, short dop, String sortColumns, short cronjobHour, boolean fromCreate, boolean disablePoormanMdamOptimization, Connection connection) throws SQLException, ParseException{
    	logger.debug(String.format("MoveFromHotToCold %s.%s",schema,tableName));
    	Connection conn = null;
        // initialize db connection
        if (connection == null){
          try {
            conn = DriverManager.getConnection( "jdbc:default:connection" );
          } catch (SQLException e) {
            throw new SQLException("Database connection failed", ERROR_DB_CONNECTION_FAILED);						
          }
        }else conn = connection;
        //get partition column name
        Statement statement=null;
        ArrayList<String> partitionsToMove = new ArrayList<String>();
            
        int stateForRollback = 0;
        String oldCutoffValue = null;
        String cutoffScalarFormula = null;
        String columnList = null;
        String columnListForUpsert = null;
        String semaphoreForCronJob = null;
        String partitionColumnName = null;
        cutoffType partitionColumnType = null ;
        String computedColumn = null;
        cutoffType columnTypeOfSourceColumn = null;
        String sourceColumn = null;
        String cutoffLiteralOnSourceColumn = null;
        String cutoffLiteralOnSourceColumnPlusOne = null;
        String newCutoffValue = null;		
        String viewTemplate=null;
        String viewTemplate2=null;
        try {
          statement = conn.createStatement();
          //get partition column
          PreparedStatement preparedStatement = conn.prepareStatement(
             "select c.column_name, c.sql_data_type, c.column_size, t.text from \"_MD_\".objects o left join \"_MD_\".columns c on o.object_uid = c.object_uid "+
             "left join \"_MD_\".keys k on o.object_uid = k.object_uid and c.column_name = k.column_name " +
             "left join \"_MD_\".text t on o.object_uid = t.text_uid and t.text_type = 4 and t.sub_id = c.column_number "+ // 4 is enum value for COMPUTED_COLUMN		
             "where catalog_name = 'TRAFODION' and o.schema_name = ? and o.object_name = ? and o.object_type = 'BT' and c.column_name <> '_SALT_' "+
             // "order by k.keyseq_number limit 1;");
             "order by k.keyseq_number ;");//workaround to 2.3 bug
          preparedStatement.setString(1, schema);
          preparedStatement.setString(2, tableName);
          ResultSet rs = preparedStatement.executeQuery();
          boolean isEmpty = true;
          //while (rs.next()){
          if (rs.next()){//workaround to 2.3 bug
                isEmpty = false;
                partitionColumnName = rs.getString(1);
                partitionColumnType = getCutoffType(rs);
                computedColumn = rs.getString(4);
          }
          rs.close();
          preparedStatement.close();
          if (isEmpty){
              //conn.close();
              throw new SQLException(String.format("Table %s.%s does not exist, or is not a hot/cold table",schema,tableName),ERROR_TABLE_NOT_FOUND);
          }
          // get current cutoff value if exists and cutoff scalar formula
          preparedStatement = conn.prepareStatement(
                  "select t.text, t.sub_id from  trafodion.\"_MD_\".objects o "
                + "join trafodion.\"_MD_\".text t ON o.object_uid = t.text_uid "
                + " and o.catalog_name='TRAFODION' and o.schema_name = ? and o.object_name = ? and o.object_type = 'BT' "
                + " and t.text_type = ? order by 2 ASC;");
          preparedStatement.setString(1, schema);
          preparedStatement.setString(2, tableName);
          preparedStatement.setInt(3, COM_HOTCOLD_TEXT);
          rs = preparedStatement.executeQuery();
          while(rs.next()){		
                switch(rs.getInt(2)){
                    case CUTOFF_SCALAR_STATEMENT:
                        cutoffScalarFormula = rs.getString(1);
                        break;
                    case COLUMN_LIST:
                        columnList = rs.getString(1);
                        break;
                    case CUTOFF_CURRENT_VALUE:
                        oldCutoffValue = rs.getString(1);
                        break;
                    case COLUMN_LIST_FOR_UPSERT:
                        columnListForUpsert = rs.getString(1);
                        break;
                    case SEMAPHORE_FOR_CRON_JOB:
                        semaphoreForCronJob = rs.getString(1);
                        break;
                    default:
                }//end switch
          }//end while	
          rs.close();
          preparedStatement.close();		
          //calculate what would be next cutoffValue
          ResultSet resultSet = statement.executeQuery(String.format("select %s from (values (1));", cutoffScalarFormula));
          resultSet.next();
          newCutoffValue = resultSet.getString(1);
          resultSet.close();
          //if oldCutoff and new cutoff are identical, do nothing
          if (newCutoffValue.equals(oldCutoffValue)) return;
          //if new cutoff and semaphore_for_cron_job are identical, it means another cron-job is already doing the work
          if (newCutoffValue.equals(semaphoreForCronJob)) return;
          logger.info(String.format("MoveFromHotToCold %s.%s started",schema,tableName));
          //retrieve list of partition to move from hot to cold
          // if we reach here, it means we are going to do the job, so grab the semaphore, so that other cron job will not re-do it while we are on it
          statement.execute("set parserflags 131072;");//turn on magic word to allow writing in _MD_ normally write protected
          preparedStatement= conn.prepareStatement(upsertHotColdTextData);
          preparedStatement.setInt(1,SEMAPHORE_FOR_CRON_JOB);
          preparedStatement.setString(2, newCutoffValue);			
          preparedStatement.setString(3, schema);
          preparedStatement.setString(4, tableName);
          int j = preparedStatement.executeUpdate();
          if (j!=1) throw (new SQLException("Failed to write Semaphore in metadata")); // should never happen
          statement.execute("reset parserflags 131072;");//turn off magic word to allow writing in _MD_
          statement.close();
          preparedStatement.close();

          // if oldCutoffValue is null, ie at creation time of hotcold, cold table is empty
          if(oldCutoffValue == null){
        	if (dop ==-1){
        		//do nothing. we don't need select distinct, as we will load all partition in one statement using the
        		// v2.4 partition repart feature of ORC and Parquet write path. This makes obsolete the code bellow
        		// but it is kept as tes bed for multi threaded JSP with T2 and T4
        	}
        	else if (!disablePoormanMdamOptimization){// would be disabled if base table has an index on it
                //simulate poor man MDAM until our compiler is able to generate MDAM for select distinct left most key column
                //optimizes a 30 minutes query into 8 minutes
            	// but this trick only works if the plan is not using index, so disable it if it is.  
                statement = conn.createStatement();
                String mdamProbe=null;
                rs = statement.executeQuery(String.format("select \"%s\" from \"%s\".\"%s\" where \"%s\" <= %s limit 1;",
                          partitionColumnName,
                          schema,
                          tableName,
                          partitionColumnName,
                          GetCutoffLiteral(partitionColumnType,newCutoffValue)));
                while(rs.next()){ // should only get 1
                    mdamProbe = rs.getString(1);
                    partitionsToMove.add(rs.getString(1)); 
                }
                rs.close();
                statement.close();
                preparedStatement = conn.prepareStatement(
                String.format("select \"%s\" from \"%s\".\"%s\" where \"%s\" <= %s and \"%s\" > ? limit 1;",
                          partitionColumnName,
                          schema,
                          tableName,
                          partitionColumnName,
                          GetCutoffLiteral(partitionColumnType,newCutoffValue),
                          partitionColumnName));
                while (mdamProbe != null){
                    switch (partitionColumnType){
                      case DATE:
                          preparedStatement.setDate(1,  new java.sql.Date(new SimpleDateFormat("yyyy-MM-dd").parse(mdamProbe).getTime()));
                          break;
                      case TIMESTAMP:
                          preparedStatement.setTimestamp(1,  new java.sql.Timestamp(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").parse(mdamProbe.substring(0,23)).getTime()));
                          break;
                      case TINYINT:
                          preparedStatement.setByte(1,Byte.parseByte(mdamProbe));
                          break;
                      case SMALLINT:
                          preparedStatement.setShort(1,Short.parseShort(mdamProbe));
                          break;
                      case INT:
                          preparedStatement.setInt(1,Integer.parseInt(mdamProbe));
                          break;
                      case BIGINT:
                          preparedStatement.setLong(1,Long.parseLong(mdamProbe));
                          break;
                      case CHAR:
                          preparedStatement.setString(1, mdamProbe);
                          break;
                      default:
                    }// end switch
                    rs = preparedStatement.executeQuery();
                    mdamProbe = null;
                    while(rs.next()){//should only get 1
                          mdamProbe = rs.getString(1);
             			  partitionsToMove.add(rs.getString(1)); 
    				}
                }// end mdam probing
                rs.close();
                preparedStatement.close();			  
            }//end poor man mdam optimization
            else
            {//regular non optimized select distinct
		        statement = conn.createStatement();		  
                System.out.println(String.format("select distinct \"%s\" from \"%s\".\"%s\" where \"%s\" <= %s;",
                      partitionColumnName,
                      schema,
                      tableName,
                      partitionColumnName,
                      GetCutoffLiteral(partitionColumnType,newCutoffValue)));
                rs = statement.executeQuery(String.format("select distinct \"%s\" from \"%s\".\"%s\" where \"%s\" <= %s;",
                      partitionColumnName,
                      schema,
                      tableName,
                      partitionColumnName,
                      GetCutoffLiteral(partitionColumnType,newCutoffValue)));
                while(rs.next()){
                  partitionsToMove.add(rs.getString(1)); 
                }
                rs.close();
                statement.close();
            }
          }else{
            // normal case, oldCutoffValue not empty... just moving cuttoff from one day to the next   
            statement = conn.createStatement();
            rs = statement.executeQuery(
                    String.format("select distinct \"%s\" from \"%s\".\"%s\" where \"%s\" > %s and \"%s\" <= %s;",
                    partitionColumnName,
                    schema,
                    tableName,
                    partitionColumnName,
                    GetCutoffLiteral(partitionColumnType,oldCutoffValue),
                    partitionColumnName,
                    GetCutoffLiteral(partitionColumnType,newCutoffValue)));
            while(rs.next()){
                partitionsToMove.add(rs.getString(1)); 
            }
            rs.close();
            statement.close();
          }//end else
          
          //copy from hot to cold 
          //pre-create partitions by batch of 35
          statement = conn.createStatement();
          System.out.print("Creating partitions...");
          StringBuilder sb = new StringBuilder();
          sb.append("process hive statement 'alter table `")
            .append(schema)
            .append("`.`")
            .append(tableName)
            .append("` add if not exists ");		
          for (int p=0; p<partitionsToMove.size(); p++){
            String value;
            if (partitionsToMove.get(p) == null)
            	value = "__HIVE_DEFAULT_PARTITION__"; // hive treatment of NULL partition. should never happen in hotcold, as NULL should always stay HOT
            else if (partitionColumnType == cutoffType.TIMESTAMP )
            	value = partitionsToMove.get(p).replace(".000000",""); // hive remove ".000000" from Timestamp on partition column
            else value = partitionsToMove.get(p); 
        	sb.append("partition (`")
        	  .append(partitionColumnName)
        	  .append("`=''")
        	  .append(value)
        	  .append("'') ");
        	if ((p%35 == 0 && p != 0) || p == (partitionsToMove.size()-1)){ // flush every 35 or when no more partition
        		sb.append("';");
        		statement.execute(sb.toString());
        		sb = new StringBuilder();
                sb.append("process hive statement 'alter table `")
                .append(schema)
                .append("`.`")
                .append(tableName)
                .append("` add if not exists ");     		
        	}      	
          }
          System.out.println("Done.");        
          statement.close();
          //end pre-create partitions
          if (dop==-1){
        	  
          }else{
          boolean parallelUpserts = true;// for now always force parallel upsert. May be in the future add this as function param
          if ((!parallelUpserts)|| partitionsToMove.size()<2 || dop == 0){//serial upsert
            statement = conn.createStatement();
            statement.execute("cqd HIVE_DATA_MOD_CHECK 'OFF';");
            if (dop != 0) {
            	statement.execute(String.format("cqd PARALLEL_NUM_ESPS '%d';",dop));// forcing DOP to limit number of files per partition
            	statement.execute("cqd HIVE_PARTITION_INSERT_REPART '0';");//make sure DOP forces number of file per partition
            	statement.execute("cqd HIVE_PARTITION_AUTO_CREATE 'OFF';");//we have pre-created partitions
            	statement.execute("cqd HIVE_PARTITION_INSERT_DIST_LOCK 'OFF';");//should not be needed given above cqd, but just in case 
            }
            System.out.println(
              String.format("upsert using load into hive.\"%s\".\"%s\" select %s from trafodion.\"%s\".\"%s\" where \"%s\" = ? %s;",
                            schema,
                            tableName,
                            columnListForUpsert,
                            schema,
                            tableName,
                            partitionColumnName,
                            sortColumns == null ? "":String.format("order by %s", sortColumns)));
            preparedStatement = conn.prepareStatement(
              String.format("upsert using load into hive.\"%s\".\"%s\" select %s from trafodion.\"%s\".\"%s\" where \"%s\" = ? %s;",
                            schema,
    				        tableName,
    				        columnListForUpsert,
    				        schema,
    				        tableName,
    				        partitionColumnName,
    				        sortColumns == null ? "":String.format("order by %s", sortColumns)));
            stateForRollback = 1; 
            for(String partitionToMove :partitionsToMove){
                if (partitionToMove == null){// special case for null partition (not really usefull for hotcold scenario, as null should stay hot and never move to cold
                  statement.executeUpdate(
                    String.format("upsert using load into hive.\"%s\".\"%s\" select %s from trafodion.\"%s\".\"%s\" where \"%s\" is null %s;",
                                  schema,
                                  tableName,
                                  columnListForUpsert,
                                  schema,
                                  tableName,
                                  partitionColumnName,
                                  sortColumns == null ? "":String.format("order by %s", sortColumns)));	
                  System.out.println(String.format("upserted partition: NULL"));
                }else{
                  switch (partitionColumnType){
                      case DATE:
                          preparedStatement.setDate(1,  new java.sql.Date(new SimpleDateFormat("yyyy-MM-dd").parse(partitionToMove).getTime()));
                          break;
                      case TIMESTAMP:
                          preparedStatement.setTimestamp(1,  new java.sql.Timestamp(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").parse(partitionToMove.substring(0,23)).getTime()));
                          break;
                      case TINYINT:
                          preparedStatement.setByte(1,Byte.parseByte(partitionToMove));
                          break;
                      case SMALLINT:
                          preparedStatement.setShort(1,Short.parseShort(partitionToMove));
                          break;
                      case INT:
                          preparedStatement.setInt(1,Integer.parseInt(partitionToMove));
                          break;
                      case BIGINT:
                          preparedStatement.setLong(1,Long.parseLong(partitionToMove));
                          break;
                      case CHAR:
                          preparedStatement.setString(1, partitionToMove);
                          break;
                      default:
                  }//end switch
                  preparedStatement.executeUpdate();
                  System.out.println(String.format("upserted partition: %s",partitionToMove));
                }//end else
            }//end for
              statement.execute("cqd HIVE_DATA_MOD_CHECK RESET;");
              if (dop != 0) {
            	  statement.execute("cqd PARALLEL_NUM_ESPS RESET;");
            	  statement.execute("cqd HIVE_PARTITION_INSERT_REPART RESET;");
              	  statement.execute("cqd HIVE_PARTITION_AUTO_CREATE RESET;");
              	  statement.execute("cqd HIVE_PARTITION_INSERT_DIST_LOCK RESET;");

              }
              statement.close();
              preparedStatement.close();
            }else{
              //parallel upsert
              // get number of partition of salted table to determine desired number of threads
              int nbThreads = 8; // this is default, defensive programing  
              statement = conn.createStatement();
              rs = statement.executeQuery(
                      String.format("select num_salt_partns from \"_MD_\".tables_view where catalog_name = 'TRAFODION' and schema_name = '%s' and table_name = '%s';",
                              schema,
                              tableName));
              while(rs.next()){
                  nbThreads = rs.getInt(1); 
              }
              rs.close();
              statement.close();			
              if (dop != 0) nbThreads = nbThreads / dop; // make sure we always end up with constant number of ESP, no matter how many files per partition we want (dop)
              Boolean success = true;
              LinkedList<String> partitionList = new LinkedList<String>();
              partitionList.addAll(partitionsToMove);
              String upsertstatement = //upsert statement for non null partitions
                      String.format("upsert using load into hive.\"%s\".\"%s\" select %s from trafodion.\"%s\".\"%s\" where \"%s\" = ? %s;",
                                    schema,
                                    tableName,
                                    columnListForUpsert,
                                    schema,
                                    tableName,
                                    partitionColumnName,
                                    sortColumns == null ? "":String.format("order by %s", sortColumns));
              System.out.println(upsertstatement);
              String upsertstatementNull = //upsert statement for the null partition
                      String.format("upsert using load into hive.\"%s\".\"%s\" select %s from trafodion.\"%s\".\"%s\" where \"%s\" is null %s;",
                                    schema,
                                    tableName,
                                    columnListForUpsert,
                                    schema,
                                    tableName,
                                    partitionColumnName,
                                    sortColumns == null ? "":String.format("order by %s", sortColumns));
              stateForRollback = 1;
              UpsertIntoPartitionTask[] upsertIntoPartitionTasks = new UpsertIntoPartitionTask[nbThreads];
              String[] type4DriverConnectionStrings = null;
              if (dop==1){ // use type4 only if required DOP is 1, else type2 with AS_AFFINITY_VALUE will correctly balance load
                  type4DriverConnectionStrings = RetrieveType4DriverConnectionStrings(); 
                  if (type4DriverConnectionStrings != null){
                    try{
                      int maxMxosrv = Integer.parseInt(type4DriverConnectionStrings[3]);
                      if (nbThreads > maxMxosrv) //adjust number number of threads to account for MXOSRV count. will keep one MXOSRV per node free for other client
                          nbThreads = maxMxosrv;
                    }catch(NumberFormatException ex){}				
                  }//end if
              }//end if
              for (int i=0; i<nbThreads ; i++){
                  // create the threads
                  upsertIntoPartitionTasks[i] = new UpsertIntoPartitionTask(partitionList,
                                                                            upsertstatement,
                                                                            upsertstatementNull,
                                                                            partitionColumnType,
                                                                            i,
                                                                            dop,
                                                                            success,
                                                                            type4DriverConnectionStrings);
                  //start the threads
                  upsertIntoPartitionTasks[i].start();
              }//end for
              int remainingThreads = 1;
              Thread.currentThread().sleep(1000);//to make sure threads had time to start before entering next loop.

              while(remainingThreads > 0){
                  //check if any of the thread exited with error, so we can instruct all thread to abort
                  remainingThreads = 0;
                  boolean isInterrupted = false;
                  for(int i=0; i<nbThreads ; i++){
                      if (upsertIntoPartitionTasks[i].t.isAlive()) remainingThreads++;
                      if (upsertIntoPartitionTasks[i].t.isInterrupted()) isInterrupted = true;
                  }//end for
                  if (isInterrupted){
                      System.err.println("Interrupting all threads");
                      for(int i=0; i<nbThreads ; i++){
                          if (upsertIntoPartitionTasks[i].t.isAlive())upsertIntoPartitionTasks[i].t.interrupt();
                      }//end for
                  }//end if                
                  Thread.currentThread().sleep(10000);
              }//end while
              if (!partitionList.isEmpty()) success = false;			
              System.out.println("Success: "+success);
              if(!success)throw new SQLException("parallel upsert into ORC failed");			
            }//end else parallel upsert
          }
          //check if we need to deal with _DIVISION_ using either date_part or date_trunc so we can add predicate on
          // the source column used in the _DIVISION_ to improve compiler ability to eliminate branches

          if (computedColumn != null){
            preparedStatement = conn.prepareStatement(
                    "select case when c.sql_data_type = 'DATETIME' AND c.column_size = 4 then 'DATE' "+
                    " when  c.sql_data_type = 'DATETIME' then 'TIMESTAMP' else 'OTHER' END from "+
                    "\"_MD_\".objects o join \"_MD_\".columns c on o.object_uid = c.object_uid AND "+ 
                    "o.catalog_name='TRAFODION' and o.schema_name = ? and o.object_name = ? and o.object_type = 'BT' "+ 
                    " and c.column_name = ?;");
            preparedStatement.setString(1, schema);
            preparedStatement.setString(2, tableName);
            sourceColumn = GetSourceColumn(computedColumn);
            preparedStatement.setString(3,sourceColumn.replaceAll("\"", ""));
            rs = preparedStatement.executeQuery();
            if(rs.next()){
              switch(rs.getString(1)){
                case "TIMESTAMP":
                    columnTypeOfSourceColumn = cutoffType.TIMESTAMP;
                    break;
                case "DATE":
                    columnTypeOfSourceColumn = cutoffType.DATE;
                    break;
                default:			
              };
            }//end if
            rs.close();
            preparedStatement.close();
            cutoffLiteralOnSourceColumn = GetCutoffLiteralOnSourceColumn(conn,computedColumn,columnTypeOfSourceColumn,newCutoffValue,false);
            cutoffLiteralOnSourceColumnPlusOne = GetCutoffLiteralOnSourceColumn(conn,computedColumn,columnTypeOfSourceColumn,newCutoffValue,true);			 
          }//end if computedColumn not null

          // update view:
          viewTemplate="create or replace view %s.%s_view (%s) as select * from %s.%s where \"%s\" > %s \n"+
                       "%s"+ //for optional predicate on source column of computed column
                       "union all select %s from hive.%s.%s where \"%s\" <= %s "+
                       "%s"; //for optional predicate on source column of computed column;
          System.out.println(String.format(viewTemplate, 
                schema,
                tableName,
                columnList,
                schema,
                tableName,
                partitionColumnName,
                GetCutoffLiteral(partitionColumnType,newCutoffValue),
                sourceColumn == null ? "":String.format("and \"%s\" >= %s \n",sourceColumn, cutoffLiteralOnSourceColumnPlusOne),
                columnList,
                schema,
                tableName,
                partitionColumnName,
                GetCutoffLiteral(partitionColumnType,newCutoffValue),
                ""//sourceColumn == null ? "":String.format("and \"%s\" < %s \n",sourceColumn, cutoffLiteralOnSourceColumnPlusOne)
                ));
          statement = conn.createStatement();
          stateForRollback = 2;
          statement.execute("cqd HIVE_NO_REGISTER_OBJECTS 'ON'");//workaround to avoid issue with T2         
          statement.execute(String.format(viewTemplate, 
                schema,
                tableName,
                columnList,
                schema,
                tableName,
                partitionColumnName,
                GetCutoffLiteral(partitionColumnType,newCutoffValue),
                sourceColumn == null ? "":String.format("and \"%s\" >= %s \n",sourceColumn, cutoffLiteralOnSourceColumnPlusOne),
                columnList,
                schema,
                tableName,
                partitionColumnName,
                GetCutoffLiteral(partitionColumnType,newCutoffValue),
                ""));//sourceColumn == null ? "":String.format("and \"%s\" < %s \n",sourceColumn, cutoffLiteralOnSourceColumnPlusOne)));
          statement.execute("cqd HIVE_NO_REGISTER_OBJECTS reset");//workaround to avoid issue with T2         

          //System.out.println("create or replace view HOTCOLDTEST.TBL3_view (\"A\",\"K2\",\"K1\",\"TS\",\"B\",\"C\") as select * from HOTCOLDTEST.TBL3 where \"_DIVISION_1_\" > TIMESTAMP '2017-09-01 00:00:00.000000' and \"TS\" >= DATEADD(MONTH,1,TIMESTAMP '2017-09-01 00:00:00.000000') ");

         // statement.execute("create or replace view HOTCOLDTEST.TBL3_view (\"A\",\"K2\",\"K1\",\"TS\",\"B\",\"C\") as select * from HOTCOLDTEST.TBL3 where \"_DIVISION_1_\" > TIMESTAMP '2017-09-01 00:00:00.000000' and \"TS\" >= DATEADD(MONTH,1,TIMESTAMP '2017-09-01 00:00:00.000000') "+
         // "");//" union all select \"A\",\"K2\",\"K1\",\"TS\",\"B\",\"C\" from hive.HOTCOLDTEST.TBL4 where \"_DIVISION_1_\" <= TIMESTAMP '2017-09-01 00:00:00.000000'");
          // create a second view with _DIVISION_ column as a temporary solution until we support computed columns on hive tables
          if (computedColumn != null){ // only create second view if partition column is a computed column (_DIVISION_).
            viewTemplate2 = "create or replace view %s.%s_view2 (%s,\"%s\") as select *,\"%s\" from %s.%s where \"%s\" > %s \n"+
                            "%s"+ //for optional predicate on source column of computed column
                            "union all select %s,\"%s\" from hive.%s.%s where \"%s\" <= %s "+
                            "%s"; //for optional predicate on source column of computed column;
            System.out.println(String.format(viewTemplate2, 
                schema,
                tableName,
                columnList,partitionColumnName,
                partitionColumnName,
                schema,
                tableName,
                partitionColumnName,
                GetCutoffLiteral(partitionColumnType,newCutoffValue),
                sourceColumn == null ? "":String.format("and \"%s\" >= %s \n",sourceColumn, cutoffLiteralOnSourceColumnPlusOne),
                columnList,
                partitionColumnName,
                schema,
                tableName,
                partitionColumnName,
                GetCutoffLiteral(partitionColumnType,newCutoffValue),
                ""
                //sourceColumn == null ? "":String.format("and \"%s\" < %s \n",sourceColumn, cutoffLiteralOnSourceColumnPlusOne)
                ));
            statement = conn.createStatement();
            stateForRollback = 3;
            statement.execute("cqd HIVE_NO_REGISTER_OBJECTS 'ON'");//workaround to avoid issue with T2  
            statement.execute(String.format(viewTemplate2, 
                schema,
                tableName,
                columnList,partitionColumnName,
                partitionColumnName,
                schema,
                tableName,
                partitionColumnName,
                GetCutoffLiteral(partitionColumnType,newCutoffValue),
                sourceColumn == null ? "":String.format("and \"%s\" >= %s \n",sourceColumn, cutoffLiteralOnSourceColumnPlusOne),
                columnList,
                partitionColumnName,
                schema,
                tableName,
                partitionColumnName,
                GetCutoffLiteral(partitionColumnType,newCutoffValue),
                ""));//sourceColumn == null ? "":String.format("and \"%s\" < %s \n",sourceColumn, cutoffLiteralOnSourceColumnPlusOne)));
            statement.execute("cqd HIVE_NO_REGISTER_OBJECTS reset");//workaround to avoid issue with T2  
          }//end if computedColumn not null

          // store new hot/cold cut-over
          stateForRollback = 4;
          statement.execute("set parserflags 131072;");//turn on magic word to allow writing in _MD_ normally write protected
          preparedStatement= conn.prepareStatement(upsertHotColdTextData);
          preparedStatement.setInt(1,CUTOFF_CURRENT_VALUE);
          preparedStatement.setString(2, newCutoffValue);			
          preparedStatement.setString(3, schema);
          preparedStatement.setString(4, tableName);
          stateForRollback = 5;
          int i = preparedStatement.executeUpdate();
          if (i!=1) throw (new SQLException("Failed to write HotCold Cutoff in metadata")); // should never happen
          stateForRollback = 6;
          statement.execute("reset parserflags 131072;");//turn off magic word to allow writing in _MD_
          stateForRollback = 7;
          preparedStatement.close();
          // setup the cron job to perform periodic move of hot cold cutoff value
          if (fromCreate){// only setup cronjob if invoked from table creation.
            System.out.println("Setting up cron job");
            SetupCronJob(schema,tableName,dop, sortColumns,cronjobHour);
            stateForRollback = 8;
            // do registration at very end to workaround issue with T2 and recursive compilation
            statement.execute(String.format("register hive table hive.\"%s\".\"%s\";",schema,tableName));
          }//end if
          statement.close();
          logger.info(String.format("MoveFromHotToCold %s.%s success",schema,tableName));
        } catch (Exception e){ //todo, there is logical bugs in this code
            if (stateForRollback>=  8){//for debuging
            //do not roll back, consider operation is successful, even if the magic parserflag is not left in goodclosed state
            }else{
                boolean successfullRollback = true;
                Statement s = null;
                try{
                  s = conn.createStatement();
                  if (stateForRollback >=7){
                     RemoveCronJob(schema,tableName);
                  }
                  if (stateForRollback >= 5){
                     s.execute("reset parserflags 131072;");//turn off magic word to allow writing in _MD_ normally write protected
                  }
                  if (stateForRollback >= 4){
                      if (computedColumn != null){
                          if (oldCutoffValue != null){//revert to previous view
                              cutoffLiteralOnSourceColumn = GetCutoffLiteralOnSourceColumn(conn,computedColumn,columnTypeOfSourceColumn,oldCutoffValue,false);
                              cutoffLiteralOnSourceColumnPlusOne = GetCutoffLiteralOnSourceColumn(conn,computedColumn,columnTypeOfSourceColumn,oldCutoffValue,true);
                              s.execute(String.format(viewTemplate2, 
                                      schema,
                                      tableName,
                                      columnList,
                                      partitionColumnName,
                                      partitionColumnName,
                                      schema,
                                      tableName,
                                      partitionColumnName,
                                      GetCutoffLiteral(partitionColumnType,oldCutoffValue),
                                      sourceColumn == null ? "":String.format("and \"%s\" >= %s \n",sourceColumn, cutoffLiteralOnSourceColumnPlusOne),
                                      columnList,
                                      partitionColumnName,
                                      schema,
                                      tableName,
                                      partitionColumnName,
                                      GetCutoffLiteral(partitionColumnType,oldCutoffValue),
                                      ""));//sourceColumn == null ? "":String.format("and \"%s\" < %s \n",sourceColumn, cutoffLiteralOnSourceColumnPlusOne)));
                          }
                          else // delete view, as there was no previous view
                              s.execute(String.format("drop view \"%s\".\"%s_VIEW2\";", schema,tableName));
                      }//end if computedColumn not null												
        		  }
                  if (stateForRollback >=3){
                      if (oldCutoffValue != null){//revert to previous view
                          cutoffLiteralOnSourceColumn = GetCutoffLiteralOnSourceColumn(conn,computedColumn,columnTypeOfSourceColumn,oldCutoffValue,false);
                          cutoffLiteralOnSourceColumnPlusOne = GetCutoffLiteralOnSourceColumn(conn,computedColumn,columnTypeOfSourceColumn,oldCutoffValue,true);
                          statement.execute(String.format(viewTemplate, 
                                  schema,
                                  tableName,
                                  columnList,
                                  schema,
                                  tableName,
                                  partitionColumnName,
                                  GetCutoffLiteral(partitionColumnType,oldCutoffValue),
                                  sourceColumn == null ? "":String.format("and \"%s\" >= %s \n",sourceColumn, cutoffLiteralOnSourceColumnPlusOne),
                                  columnList,
                                  schema,
                                  tableName,
                                  partitionColumnName,
                                  GetCutoffLiteral(partitionColumnType,oldCutoffValue),
                                  ""));//sourceColumn == null ? "":String.format("and \"%s\" < %s \n",sourceColumn, cutoffLiteralOnSourceColumnPlusOne)));
                      }//end if oldCutoffValue not null
                      else // delete view, as there was no previous view
                          s.execute(String.format("drop view \"%s\".\"%s_VIEW\";", schema,tableName));					
                 }
                 if (stateForRollback >= 2){
                    for(String partitionToMove :partitionsToMove){						
                        boolean needQuotes;					    
                        switch (partitionColumnType){
                            case DATE:
                            case TIMESTAMP:
                            case CHAR:
                                needQuotes = true;
                                break;
                            default:
                                needQuotes = false;
                        }
                        if (needQuotes)
                            s.execute(String.format("process hive statement 'ALTER TABLE `%s`.`%s` DROP IF EXISTS PARTITION (`%s`=''%s'') PURGE';", 
                                    schema,tableName,partitionColumnName,partitionToMove));
                        else
                            s.execute(String.format("process hive statement 'ALTER TABLE `%s`.`%s` DROP IF EXISTS PARTITION (`%s`=%s) PURGE';", 
                                    schema,tableName,partitionColumnName,partitionToMove));
                    }//end for
                 }
                 s.execute("CQD HIVE_DATA_MOD_CHECK RESET;");
                 if (dop != 0){
                	 s.execute("CQD PARALLEL_NUM_ESPS RESET;");
                	 s.execute("CQD HIVE_PARTITION_INSERT_REPART RESET;");
                 	 s.execute("cqd HIVE_PARTITION_AUTO_CREATE RESET;");
                	 s.execute("cqd HIVE_PARTITION_INSERT_DIST_LOCK RESET;"); 

                 }
                 s.close();
               }catch(Exception ex){
                   successfullRollback = false;
                   if (s != null) s.close();
                   //if(conn!=null) conn.close();
               }
               e.printStackTrace(System.out);

               if (successfullRollback){
            	   logger.error(String.format("MoveToCold failed with successful rollback. exception: %s",buildSQLExceptionMessage(e)));
                   throw  new SQLException("HotCold MOVETOCOLD operation failed, and successfully rolled back. Original exception :\n"+buildSQLExceptionMessage(e),ERROR_MOVETOCOLD_FAILED_AND_ROLLEDBACK);
               }
               else{
            	   logger.error(String.format("MoveToCold failed with failed rollback. exception: %s",buildSQLExceptionMessage(e)));
            	   throw new SQLException("Failed to rollback. please run CALL trafodion.\"_LIBMGR_\".hotcold_cleanup(schemaname, tablename) to get back to a clean state.\n Original failure: "+
            		                        buildSQLExceptionMessage(e),ERROR_ROLLBACK_FAILED);
               }

            }
        }
    }
    
    //cleanup remove left over from failed attempt to create hotcold 
    //mode:
    //NULL or 'DEFAULT': remove hotcold related _MD_ objects, views and cronjobs
    //        'DROP_COLD': 'DEFAULT' + drop the COLD table. warning, if you  data from hot, you will loose data
    //                     if you DROP_COLD without having first moved data back from COLD to HOT
    static public void Cleanup(String primarySchemaIn, String primaryTableIn, String modeIn ) throws SQLException{
        if ((modeIn == null) || primarySchemaIn == null || primaryTableIn == null){
            System.out.println("Invalid parameter: No null string allowed\n");
            return;
        }
    	String primarySchema = 	primarySchemaIn.toUpperCase();
    	String primaryTable = primaryTableIn.toUpperCase();
    	String mode = modeIn.toUpperCase();
        //validate mode
        if ((mode != null) && (!mode.equals("DEFAULT")) && (!mode.equals("DROP_COLD"))){
            System.out.println("Invalid mode: can be DEFAULT or DROP_COLD.\n");
            return;
        }
        //remove cronjob
        try{ 
            System.out.print("Removing cronjobs ...");
            RemoveCronJob(primarySchema,primaryTable);
            System.out.println("done");
        }catch(Exception e){
        	    logger.error(String.format("Remove cronjob failed with exception: %s", e.getMessage()));
                System.out.println(String.format("Remove cronjob failed with exception: %s", e.getMessage()));
        }
        //clean up METADATA
        Connection conn = null;
        // initialize db connection
        try {
            conn = DriverManager.getConnection( "jdbc:default:connection" );
        } catch (SQLException e) {
            throw new SQLException("Database connection failed", ERROR_DB_CONNECTION_FAILED);                       
        }
        Statement statement = null;
        PreparedStatement preparedStatement = null;
        try{
            statement = conn.createStatement();
            System.out.print("Dropping view(s)  ...");
            try{
            	statement.execute("cqd HIVE_NO_REGISTER_OBJECTS 'ON';");
                statement.execute(String.format("drop view \"%s\".\"%s_VIEW\";", primarySchema,primaryTable));
            }catch(SQLException ee){
                logger.error(String.format("dropping view failed with exception: %s",ee.getMessage()));            	
                System.out.println(String.format("dropping view failed with exception: %s",ee.getMessage()));
            }            
            try{
                statement.execute(String.format("drop view \"%s\".\"%s_VIEW2\";", primarySchema,primaryTable));
            }catch(SQLException ee){}//ignore failure of VIEW2 removal, as VIEW2 may not exist.
            statement.execute("cqd HIVE_NO_REGISTER_OBJECTS RESET;");
            System.out.println("done");
            System.out.print("Deleting metadata ...");            
            statement.execute("set parserflags 131072;");//turn on magic word to allow writing in _MD_ normally write protected
            preparedStatement= conn.prepareStatement(
               "delete from \"_MD_\".TEXT where text_uid = (select object_uid from \"_MD_\".OBJECTS "+ 
               "where catalog_name = 'TRAFODION' and schema_name = ? and object_name = ? and object_type = 'BT') and text_type=?;");
            preparedStatement.setString(1, primarySchema);
            preparedStatement.setString(2, primaryTable);
            preparedStatement.setInt(3,COM_HOTCOLD_TEXT);
            try{
                int i = preparedStatement.executeUpdate();
            }catch(SQLException ee){
                logger.error(String.format("dropping metadata failed with execption: %s",ee.getMessage()));
            	System.out.println(String.format("dropping metadata failed with execption: %s",ee.getMessage()));
            }            
            statement.execute("reset parserflags 131072;");//turn off magic word to allow writing in _MD_ normally write protected            
            System.out.println("done");
            if (mode != null && mode.equals("DROP_COLD")){
               //drop cold table and possibly cold schema
               // try{
                //	System.out.print("unregister cold table ...");
                //	statement.execute(String.format("unregister hive table hive.\"%s\".\"%s\";",primarySchema,primaryTable));
                //}catch(SQLException ee){};
               // System.out.println("done");                
            	//try{
                	//System.out.print("Cleanup cold table metadata ...");
                	//statement.execute(String.format("cleanup hive table \"%s\".\"%s\";",primarySchema,primaryTable));
                //}catch(SQLException ee){};
                //System.out.println("done");  
                System.out.print("Dropping cold table ...");
                statement.execute(String.format("process hive statement 'DROP TABLE IF EXISTS `%s`.`%s` PURGE';",primarySchema, primaryTable));             
                System.out.println("done"); 

                try{
                  System.out.print("Try dropping cold schema ...");
                  statement.execute(String.format("process hive statement 'DROP SCHEMA IF EXISTS `%s` RESTRICT';",primarySchema));                  
                }catch(SQLException ee){};//ignore case when schema cannot be dropped because table still exists
                System.out.println("done");
                
                try{
                	System.out.print("unregister cold table ...");
                	statement.execute(String.format("unregister hive table hive.\"%s\".\"%s\" cleanup;",primarySchema,primaryTable));
                }catch(SQLException ee){};
                System.out.println("done");   
                
              
            }//end if DROP_COLD
        } catch (SQLException e){
        	logger.error(String.format("Failed to cleanup with exception: %s", e.getMessage()));
            throw new SQLException("Failed to cleanup", ERROR_CLEANUP_FAILED);
        } finally{
            if (statement != null) statement.close();
            if (preparedStatement != null) preparedStatement.close();
            //if (conn != null) conn.close();
        }
    }
	
    //return cutoffType based on SQL_DATA_TYPE and COLUMN_SIZE column in resultSet
    private  static cutoffType getCutoffType(ResultSet rs) throws SQLException {
        switch (rs.getString("SQL_DATA_TYPE").trim()){
            case "DATETIME":
                if(rs.getInt("COLUMN_SIZE") == 4){
                    return cutoffType.DATE;
                }else{
                    return cutoffType.TIMESTAMP;
                }
            case "CHARACTER":
            case "VARCHAR":
                return cutoffType.CHAR;
            case "SIGNED TINYINT":
            case "UNSIGNED TINYINT":
                return cutoffType.TINYINT;
            case "UNSIGNED SMALLINT":
            case "SIGNED SMALLINT":
                return cutoffType.SMALLINT;
            case "SIGNED INTEGER":
            case "UNSIGNED INTEGER":
                return cutoffType.INT; 			
            case "SIGNED LARGEINT":
                return cutoffType.BIGINT;
            default:
                return cutoffType.INT;
        }//end switch		
    }
    
    // this function is to determine how to decorate the cutoff scalar
    // with '', or TIMESTAMP or DATE, or nothing.
    private static String GetCutoffLiteral(cutoffType ctype, String cutoffLiteral){
        switch (ctype){
            case CHAR:
                return String.format("'%s'", cutoffLiteral);
            case TIMESTAMP:
                return String.format("TIMESTAMP '%s'", cutoffLiteral);
            case DATE:
                return String.format("DATE '%s'", cutoffLiteral);
            default:
                return cutoffLiteral;				
        }
    }

    //extract column name from computed column string
    // example: input: DATE_TRUNC('MONTH',TS) output: TS
    private static String GetSourceColumn(String computedColumn){
        if (computedColumn == null) return null;
        if (computedColumn.contains("DATE_TRUNC")){
            String pattern = "(.*)(DATE_TRUNC\\('\\w*'\\,)(.*)(\\).*)";
            return computedColumn.replaceAll(pattern, "$3");
        } else if (computedColumn.contains("DATE_PART")){
            String pattern = "(.*)(DATE_PART\\('\\w*'\\,)(.*)(\\).*)";
            return computedColumn.replaceAll(pattern, "$3");			
        } else return null;
    }
	
    // extract the literal associated with the DATE_TRUNC or DATE_PART used in DIVISION_BY
    // plusOne boolean is used to instruct to compute next boundary
    private static String GetCutoffLiteralOnSourceColumn(Connection conn, String computedColumn,cutoffType sourceColumnType,String cutoffLiteral, boolean plusOne) throws SQLException{
        if (computedColumn == null) return null;
        if (computedColumn.contains("DATE_TRUNC")){
            if (plusOne){
                String pattern = "(.*)(DATE_TRUNC\\(')(\\w*)('\\,.*\\))(.*)";
                String extracted = computedColumn.replaceAll(pattern, "$3");
                if(extracted.equals("YEAR") || extracted.equals("MONTH") || extracted.equals("DAY") || extracted.equals("HOUR"))
                    return (String.format("DATEADD(%s,1,%s)", extracted,GetCutoffLiteral(sourceColumnType,cutoffLiteral)));
                else throw new SQLException("HotCold feature only support DIVISION_BY DATE_TRUNC with YEAR, MONTH, DAY or HOUR: not "+extracted);
            }else
                return GetCutoffLiteral(sourceColumnType,cutoffLiteral);		
        }else if (computedColumn.contains("DATE_PART")){
            //retrieve type of source column from DATE_PART
            String pattern = "(.*)(DATE_PART\\(')(\\w*)('\\,.*\\))(.*)";
            String extracted = computedColumn.replaceAll(pattern, "$3");
            switch (extracted){
                case "YEAR":
                    if (plusOne){
                        if (sourceColumnType == cutoffType.TIMESTAMP){
                            return String.format("DATEADD(YEAR,1,%s)", GetCutoffLiteral(sourceColumnType,String.format("%s-01-01 00:00:00.000000", cutoffLiteral.substring(0,4))));
                        }else{
                            return String.format("DATEADD(YEAR,1,%s)", GetCutoffLiteral(sourceColumnType,String.format("%s-01-01", cutoffLiteral.substring(0,4))));					
                        }					
                    }else{
                        if (sourceColumnType == cutoffType.TIMESTAMP){
                            return GetCutoffLiteral(sourceColumnType,String.format("%s-01-01 00:00:00.000000", cutoffLiteral.substring(0,4)));
                        }else{
                            return GetCutoffLiteral(sourceColumnType,String.format("%s-01-01", cutoffLiteral.substring(0,4)));					
                        }
                    }
                case "YEARQUARTER":
                    String monthStr = null;
                    int year = Integer.parseInt(cutoffLiteral.substring(0,4));
                    switch(cutoffLiteral.substring(4,5)){
                        case "1":					
                            monthStr = plusOne?"04":"01";
                            break;
                        case "2":
                            monthStr = plusOne?"07":"04";
                            break;
                        case "3":
                            monthStr = plusOne?"10":"07";
                            break;
                        case "4":
                            monthStr = plusOne?"01":"10";
                            if (plusOne) year ++;
                            break;
                        default:
                    }
                    if (sourceColumnType == cutoffType.TIMESTAMP){
                        return GetCutoffLiteral(sourceColumnType,String.format("%d-%s-01 00:00:00.000000", year,monthStr));
                    }else{
                        return GetCutoffLiteral(sourceColumnType,String.format("%d-%s-01", year,monthStr));					
                    }
                case "YEARMONTH":
                    int month = Integer.parseInt(cutoffLiteral.substring(4,6));
                    year = Integer.parseInt(cutoffLiteral.substring(0,4));
                    if (plusOne){ //increment one month
                        if (month == 12){
                            month = 1; year ++;						
                        }else
                            month++;
                    }
                    if (sourceColumnType == cutoffType.TIMESTAMP){
                        return GetCutoffLiteral(sourceColumnType,String.format("%d-%02d-01 00:00:00.000000", year,month));
                    }else{
                        return GetCutoffLiteral(sourceColumnType,String.format("%d-%02d-01", year, month));					
                    }				
                case "YEARWEEK":
                    //brute force compute of reverse function of DATE_PART YEARWEEK
                    Statement statement = conn.createStatement();
                    ResultSet rs = statement.executeQuery(String.format("select distinct aa,b from ("+
                                      "select first_value(a) over (partition by b order by a) aa, b from("+
                                      "select dateadd(DAY,d,date %s '%s-01-01%s') a, DATE_PART('YEARWEEK',dateadd(DAY,d,date '2017-01-01')) b "+
                                      "from (select 100*x100+10*x10+x1 as d "+
                                      "from (values (0)) "+
                                      "transpose 0,1,2,3,4,5,6,7,8,9 as x1 "+
                                      "transpose 0,1,2,3,4,5,6,7,8,9 as x10 "+
                                      "transpose 0,1,2,3 as x100 "+		  
                                      ") where d <= 365 order by 1)); ",
                                      sourceColumnType == cutoffType.TIMESTAMP?"TIMESTAMP":"DATE",
                                      cutoffLiteral.substring(0,4),//extract year of YYYYWW
                                      sourceColumnType == cutoffType.TIMESTAMP? " 00:00:00.000000":""));
                    String literal = null;
                    while(rs.next()){
                        if (rs.getInt(2) == Integer.parseInt(cutoffLiteral)){
                            if (plusOne) rs.next();//get next value after the match                                
                            literal = GetCutoffLiteral(sourceColumnType, rs.getString(1));
                        }
                    }
                    rs.close();
                    statement.close();
                    return literal;
                default:
                    return null;
            }//end switch
        }//end else if DATE_PART
        return null;//should never happen b
    }
	
    private  static void addColumnOrcTableDDL(StringBuilder sb, ResultSet rs,HashMap<String,String> columnNames, boolean isORC) throws Exception{
        int nbChar = 0;
        int columnPrecision = rs.getInt("COLUMN_PRECISION");
        int columnScale = rs.getInt("COLUMN_SCALE");
        String columnName = rs.getString("COLUMN_NAME");
        //check if columnName was not a quoted column name
        if (!columnName.toUpperCase().equals(columnName) || columnName.contains(".") || columnName.contains(",") )
            throw (new SQLException ("Hot/Cold unsupported \" column name " + rs.getString("COLUMN_NAME").trim(),ERROR_UNSUPPORTED_SCHEMA_TABLE_COLUMN_NAME));

        String columnNameToLower = columnName.toLowerCase();		
        columnNames.put(columnNameToLower, columnNameToLower);
        sb.append('`').append(columnName).append("` ");
        switch (rs.getString("SQL_DATA_TYPE").trim()){
            case "SIGNED TINYINT":
                if (columnPrecision > 0)
                    sb.append("DECIMAL(").append(columnPrecision).append(',').append(columnScale).append(')');
                else
                    sb.append("TINYINT");
                break;
            case "UNSIGNED TINYINT":
                if (columnPrecision > 0)
                    sb.append("DECIMAL(").append(columnPrecision).append(',').append(columnScale).append(')');
                else
                    sb.append("SMALLINT");
                break;			
            case "UNSIGNED SMALLINT":
                if (columnPrecision > 0)
                    sb.append("DECIMAL(").append(columnPrecision).append(',').append(columnScale).append(')');
                else
                    sb.append("INT");
                break;
            case "SIGNED SMALLINT":
                if (columnPrecision > 0)
                    sb.append("DECIMAL(").append(columnPrecision).append(',').append(columnScale).append(')');
                else
                    sb.append("SMALLINT");
    			break;
    		case "SIGNED INTEGER":
                if (columnPrecision > 0)
                    sb.append("DECIMAL(").append(columnPrecision).append(',').append(columnScale).append(')');
                else
    		        sb.append("INT");
    		    break;			
    		case "UNSIGNED INTEGER":
                if (columnPrecision > 0)
                    sb.append("DECIMAL(").append(columnPrecision).append(',').append(columnScale).append(')');
                else
    		        sb.append("BIGINT");
    		    break;			
    		case "SIGNED LARGEINT":
                if (columnPrecision > 0)
                    sb.append("DECIMAL(").append(columnPrecision).append(',').append(columnScale).append(')');
                else
    		        sb.append("BIGINT");
    		    break;
    		case "CHARACTER":
    		    String characterset = rs.getString("CHARACTER_SET").trim();
    		    if (characterset.equals("ISO88591"))
    		        nbChar = rs.getInt("COLUMN_SIZE");
    		    else if (characterset.equals("UTF8")){
                    if (rs.getInt("COLUMN_PRECISION") != 0)
                      nbChar= rs.getInt("COLUMN_PRECISION");// case when UTF8 expressed as CHAR(x) or CHAR(x chars)
                    else 
                      nbChar = rs.getInt("COLUMN_SIZE");// case when UTF8 expressed as CHAR(x bytes)
                }
                else if (characterset.equals("UCS2"))
                    throw (new SQLException ("Hot/Cold unsupported character type for column " + rs.getString("COLUMN_NAME").trim(),ERROR_UNSUPPORTED_TYPE));
                   // nbChar= rs.getInt("COLUMN_SIZE")/2;
                else throw (new Exception("Error Unkown character set"));
                //HIVE CHAR type max size is 255. so map to CHAR if <= 255 else map to VARCHAR
                if (nbChar <= 255)
                    sb.append("CHAR(").append(nbChar).append(')');
                else if (nbChar <= 65535)
                    sb.append("VARCHAR(").append(nbChar).append(')');
                else throw (new Exception("Error max CHAR supported is 65535"));
                break;
            case "VARCHAR":
                characterset = rs.getString("CHARACTER_SET").trim();
                if (characterset.equals("ISO88591"))
                    nbChar = rs.getInt("COLUMN_SIZE");
                else if (characterset.equals("UTF8")){
                    if (rs.getInt("COLUMN_PRECISION") != 0)
                      nbChar= rs.getInt("COLUMN_PRECISION");// case when UTF8 expressed as CHAR(x) or CHAR(x chars)
                    else 
                      nbChar = rs.getInt("COLUMN_SIZE");// case when UTF8 expressed as CHAR(x bytes)
                }
                else if (characterset.equals("UCS2"))
                    throw (new SQLException ("Hot/Cold unsupported character type for column " + rs.getString("COLUMN_NAME").trim(),ERROR_UNSUPPORTED_TYPE));
                    //nbChar= rs.getInt("COLUMN_SIZE")/2;
                else throw (new Exception("Error Unkown character set"));
                //HIVE CHAR type max size is 255. so map to CHAR if <= 255 else map to VARCHAR
                if (nbChar <= 65535)
                    sb.append("VARCHAR(").append(nbChar).append(')');
                else throw (new Exception("Error max VARCHAR supported is 65535"));			
                break;
            case "REAL":
                sb.append("FLOAT");
                break;
            case "DOUBLE":
                sb.append("DOUBLE");
                break;
            case "SIGNED DECIMAL":
            case "UNSIGNED DECIMAL":
            case "SIGNED NUMERIC":
            case "UNSIGNED NUMERIC":            	
            	sb.append("DECIMAL(").append(columnPrecision).append(',').append(columnScale).append(')');
                break;
            case "DATETIME":
                if(rs.getInt("COLUMN_SIZE") == 3 || //TIME(0)
                   (rs.getInt("COLUMN_SIZE") == 7 && columnScale > 0)) //TIME(1)..TIME(9)
                    throw (new SQLException ("Hot/Cold unsupported type for column " + rs.getString("COLUMN_NAME").trim(),ERROR_UNSUPPORTED_TYPE));
                else if(rs.getInt("COLUMN_SIZE") == 4){ //DATE
                    if(isORC)
                       sb.append("DATE");
                    else
                       throw (new SQLException ("Hot/Cold DATE type only supported for ORC COLD table: column " + rs.getString("COLUMN_NAME").trim(),ERROR_UNSUPPORTED_TYPE));
                }
                else
                    sb.append("TIMESTAMP");                
                break;
            default:
                throw (new SQLException ("Hot/Cold unsupported type for column " + rs.getString("COLUMN_NAME").trim(),ERROR_UNSUPPORTED_TYPE));
        }//end switch
    }
	
    //Setup the cron job that will check if data need to be moved from hot to cold
    //the cron job will be installed on all nodes, up to 29, therefore insuring high availability in case of node down
    //Complexity of this function is due to Esgyn install without password-less ssh, and a bug in edp_pdsh that is swallowing ','
    //The trick is to write a script in hdfs (available to all nodes)
    //edb_pdsh the sript in HDFS so it is ran on all nodes
    //that script adds a cronjob using crontab, and make sure it doesn't add duplicates
    //it also make sure each node run the script 2 mn apart using a constructed NODE_INDEX trick
    //delete the temp script written in hdfs
    //intput: schemaName, tableName, hour. Use -1 for hour if you want the job to run every hour, else specify the hour in the
    //day, from 0 to 24 when you want the job to run.
    private static void SetupCronJob(String schemaName, String tableName, short dop, String sortColumns, int hour) throws IOException,InterruptedException{
      String hourStr="*";
      if (hour>=0 && hour<=23)
          hourStr=Integer.toString(hour);
      String tmpFileName = UUID.randomUUID().toString();
      String cmdOnEachNode = 
    "source ~/.bashrc\n"+
    "(echo \"source ~/.bashrc\";\n"+
    "echo \"NODE_INDEX=\\$(trafconf -wname | sed 's/ /\\n/g' | nl -n ln -p | grep -E  \\\"([[:space:]]\\$(hostname -s)\\.|[[:space:]]\\$(hostname -s)\\$)\\\" | cut -f 1)\";\n"+
    "echo \"if [ \\$[NODE_INDEX] -le 59 ]; then\";\n"+
//    "echo \"    eval \\\"(crontab -l ; echo \\$NODE_INDEX'%s * * * (source ~/.bashrc;echo \\\\\\\"call trafodion.\\\\\\\\\\\\\\\"_LIBMGR_\\\\\\\\\\\\\\\".hotcold_movetocold('\\\\\\\"'\\\\\\\"'%s'\\\\\\\"'\\\\\\\"','\\\\\\\"'\\\\\\\"'%s'\\\\\\\"'\\\\\\\"',%d,%s);\\\\\\\"|sqlci)>/dev/null') 2>&1 | grep -v 'no crontab' | sort | uniq | crontab -\\\"\";\n"+
    "echo \"    eval \\\"(crontab -l ; echo \\$NODE_INDEX'%s * * * (source ~/.bashrc;hotcold_movetocold %s %s %d %s)>/dev/null') 2>&1 | grep -v 'no crontab' | sort | uniq | crontab -\\\"\";\n"+
    "echo \"fi\")|hdfs dfs -put - %s\n"+
    "edb_pdsh -a \"hdfs dfs -cat %s|exec sh\"\n"+
    "hdfs dfs -rm -skipTrash %s\n"; 
      if (sortColumns != null)
          sortColumns = sortColumns.replaceAll("\"", "\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\"");// yes, this is a lot of escaping ...
      cmdOnEachNode = String.format(
              cmdOnEachNode,
              hourStr,
              schemaName,
              tableName,
              dop,
//              sortColumns == null ? "NULL" : String.format("'\\\\\\\"'\\\\\\\"'%s'\\\\\\\"'\\\\\\\"'", sortColumns),
              sortColumns == null ? "" : sortColumns,
              tmpFileName,tmpFileName,tmpFileName);
      String sourcePath[] = {"/bin/bash","-c",cmdOnEachNode};
      try {
        Process p = Runtime.getRuntime().exec(sourcePath);
        p.waitFor();
        BufferedReader b = new BufferedReader(new InputStreamReader(p.getInputStream()));
        String line = "";
        while ((line = b.readLine()) != null) {}
        b.close();	
      } 
      catch (Exception e) {
        System.err.println("Failed to execute bash with command: " + cmdOnEachNode);
        e.printStackTrace();
        throw e;
      }
    }

    // Remove the cron job in case we need to roll back.
    // refers to SetupConJob to understand the mechanics of this function.
    private static void RemoveCronJob(String schemaName, String tableName) throws IOException {
      String tmpFileName = UUID.randomUUID().toString();
      String cmdOnEachNode = 
    "source ~/.bashrc\n"+		  
    "(echo \"source ~/.bashrc\";\n"+
    "echo \"NODE_INDEX=\\$(trafconf -wname | sed 's/ /\\n/g' | nl -n ln -p | grep -E  \\\"([[:space:]]\\$(hostname -s)\\.|[[:space:]]\\$(hostname -s)\\$)\\\" | cut -f 1)\";\n"+
    "echo \"if [ \\$[NODE_INDEX] -le 59 ]; then\";\n"+
//    "echo \"    eval \\\"crontab -l 2>&1 | grep -v 'no crontab' |grep -v '.hotcold_movetocold('\\\\\\\"'\\\\\\\"%s\\\\\\\"','\\\\\\\"%s\\\\\\\"'\\\\\\\"| sort | uniq | crontab -\\\"\";\n"+    
    "echo \"    eval \\\"crontab -l 2>&1 | grep -v 'no crontab' |grep -v 'hotcold_movetocold %s %s'| sort | uniq | crontab -\\\"\";\n"+    
    "echo \"fi\")|hdfs dfs -put - %s\n"+
    "edb_pdsh -a \"hdfs dfs -cat %s|exec sh\"\n"+
    "hdfs dfs -rm -skipTrash %s\n"; 
      cmdOnEachNode = String.format(cmdOnEachNode,schemaName,tableName,tmpFileName,tmpFileName,tmpFileName);
      String sourcePath[] = {"/bin/bash","-c",cmdOnEachNode};
      try {
        Process p = Runtime.getRuntime().exec(sourcePath);
        p.waitFor();
        BufferedReader b = new BufferedReader(new InputStreamReader(p.getInputStream()));
        String line = "";
        while ((line = b.readLine()) != null) {}
        b.close();
      }
      catch (Exception e) {
        System.err.println("Failed to execute bash with command: " + cmdOnEachNode);
        e.printStackTrace();
      }
    }
	
    //Retrieve type 4 driver connection string, user name, password and maximum number of MXO server we can use in parallel.
    //if authentication has been enabled, you can only use hotcold create with dop 1 from sqlci and pass db__root
    //password on a file "hotcoldpwd" that you will delete right after the hotcold create is complete.
    //Use dcscheck to retrieve all info needed to build jdbc URL, and reserve up to 80% of active MXOSRVR.
    //returns an array of 4 strings,0: host,1: user,2: password, 3: max number of mxosrvr to use, and null in case of error.
    private static String[] RetrieveType4DriverConnectionStrings(){
        String[] result = new String[4];
		//get password from fixed location. this is temporary solution until we have way to securelly invoke T4 from SPJ without password
		//user will have to invoke hotcold from sqlci, and store db__root password in a file named hotcoldpwd. then delete it after hotcold create was invoked.
        try{
            InputStream is = new FileInputStream("hotcoldpwd");//hardcoded name to provide password. to be used with sqlci
        	BufferedReader buf = new BufferedReader(new InputStreamReader(is));
        	result[2] = buf.readLine();
        	System.out.println("db__root password provided");
            buf.close();
            is.close();			
        }catch(IOException e){
		    result[2] = "dummy";
        }
        try{
            String dcscheckPath = System.getenv("TRAF_HOME")+"/sql/scripts/dcscheck";
            String[] configuredDCSMasters = null;
            String listenPort = null;
            String mxosrvrCount = null; 
            try{
               Process proc = Runtime.getRuntime().exec(dcscheckPath);
               BufferedReader read = new BufferedReader(new InputStreamReader(proc.getInputStream()));
               try{proc.waitFor();}
               catch(InterruptedException e){
                System.out.println(e.getMessage());
               }              
               while(read.ready()){
                  String line = read.readLine();
                  if (line.startsWith("Configured DcsMaster")){
                    configuredDCSMasters = line.substring(line.indexOf(":")+1).trim().split(" ");
                  }else if (line.startsWith("DcsMaster listen port")){
                    listenPort = line.substring(line.indexOf(":")+1).trim();
                  }else if (line.startsWith("mxosrvr")){
                    mxosrvrCount = line.split("\t")[4];
                  }
               }
            }catch (IOException e){
                System.out.println(e.getMessage());
                result = null;
                return result;
            }
            if (listenPort == null || configuredDCSMasters == null){
                System.out.println("dcscheck script problem");
                result=null;
            }else{
                StringBuilder sb = new StringBuilder();
                boolean isFirst = true;
                for (String s : configuredDCSMasters){
                  if (!isFirst) sb.append(',');
                  sb.append(s)
                    .append(':')
                    .append(listenPort);                                   
                  isFirst = false;
                }
                result[0] = sb.toString();
                result[1] = "db__root";
                result[3] = Integer.toString((Integer.parseInt(mxosrvrCount) * 80)/100);             
            }            
        }catch (Exception e){
            System.out.println(e.getMessage());
            result=null;
        }   
        return result;        
    }

	
    // multithreading code
    //====================
    // Task executing upsert requests into partition 
    // if dop = 1 and correct type4Driver connection strings are passed, we will try using type 4 driver
    // for all other cases, we fall back on Type2 driver using AS_AFFITITY_VALUE -1 to randomize working ESPs accross nodes
    protected static class UpsertIntoPartitionTask implements Runnable {
      private Thread t;
      private String threadName;		 
      private LinkedList<String> partitionsToMove;
      private String upsertStatement;
      private String upsertStatementNull;
      private int taskNb;
      private Boolean success;
      private cutoffType partitionColumnType;
      private int dop;
      private String[] type4DriverConnectionStrings;
      public UpsertIntoPartitionTask(LinkedList<String> partitionsToMove,
                                     String upsertStatement,
                                     String upsertStatementNull, 
                                     cutoffType partitionColumnType,                                     
                                     int taskNb, 
                                     int dop, 
                                     Boolean success,
                                     String[] type4DriverConnectionStrings) {
        this.partitionsToMove = partitionsToMove;
        this.upsertStatement = upsertStatement;
        this.upsertStatementNull = upsertStatementNull;
        this.success = success;
        this.partitionColumnType = partitionColumnType;
        this.dop = dop;
        this.taskNb = taskNb;
        this.type4DriverConnectionStrings = type4DriverConnectionStrings;
        this.threadName = String.format("upsertIntoPartitionThread-%d", taskNb);
      }

      public void start(){
        System.out.println("Starting "+threadName);
        if (t == null){
            t = new Thread (this, threadName);
            t.start();
        }
      }

      @Override
      public void run() {
        boolean type4driver = true;
        type4driver = (type4DriverConnectionStrings != null);
        try{
          Properties props = new Properties();
          String dbserver = null;
          if (type4driver){
              props.setProperty("catalog","trafodion");
              props.setProperty("user", type4DriverConnectionStrings[1]);
              props.setProperty("password", type4DriverConnectionStrings[2]);
              dbserver = type4DriverConnectionStrings[0];
          }
          Connection conn = null;
          Statement statement = null;
          PreparedStatement preparedStatement = null;
          try{
              if (type4driver) 
            	  Class.forName("org.trafodion.jdbc.t4.T4Driver");
              else 
            	  Class.forName("org.apache.trafodion.jdbc.t2.T2Driver");
          } catch (Exception e) {
              success=false;
              System.out.println(String.format("Thread %d Exception:%s",taskNb,e.toString()));
              throw new SQLException("T4 driver not found", ERROR_TRAFODION_DRIVER_NOT_FOUND);						
          }			
          try {
              if (type4driver)
                  conn = DriverManager.getConnection(String.format("jdbc:t4jdbc://%s/:",dbserver), props);
              else{
            	  
                  //conn = DriverManager.getConnection( "jdbc:default:connection" );
            	  conn = DriverManager.getConnection( "jdbc:t2jdbc:" );
            	  }
          } catch (SQLException e) {
              success=true; // if we cannot connect one thread that is OK. so keep success on. it may well be because we are running out of MXOSRVR
              System.out.println(String.format("Thread %d Exception:%s",taskNb,e.toString()));
              logger.error(String.format("Database connection failed Thread %d Exception:%s",taskNb,e.toString()));
              throw new SQLException("Database connection failed", ERROR_DB_CONNECTION_FAILED);						
          }
          String partitionToMove = null;
          try {
              statement = conn.createStatement();
              statement.execute("cqd HIVE_DATA_MOD_CHECK 'OFF';");
              if (dop != 0) {
            	  statement.execute(String.format("cqd PARALLEL_NUM_ESPS '%d';",dop));// forcing DOP to limit number of files per partition
              	  statement.execute("cqd HIVE_PARTITION_INSERT_REPART '0';");//make sure DOP forces number of file per partition 
              	  statement.execute("cqd HIVE_PARTITION_AUTO_CREATE 'OFF';");//we have pre-created partitions
              	  statement.execute("cqd HIVE_PARTITION_INSERT_DIST_LOCK 'OFF';");//should not be needed given above cqd, but just in case 

              }
              if (!type4driver) statement.execute("cqd AS_AFFINITY_VALUE '-3';");// if type2, use random assignment of ESP to unsure load balancing
                                                                                 // no needed for type4, as load balancing is achieve by hitting different MXOSVR
              statement.execute("cqd HBASE_NUM_CACHE_ROWS_MAX '10000';");
              preparedStatement = conn.prepareStatement(upsertStatement);
              boolean endOfList = false;
              synchronized (partitionsToMove){
                try{
                  partitionToMove = partitionsToMove.removeFirst();
                }catch(NoSuchElementException e){
                  endOfList = true;
                }
              }//end synchronized
              while(!endOfList && !t.isInterrupted() && success) {
                if (partitionToMove == null){ //special case for null partition                      
                  statement.executeUpdate(upsertStatementNull);
                  System.out.println(String.format("upserted partition: NULL with thread %d",taskNb));
                }else{
                  switch (partitionColumnType){
                    case DATE:
                      preparedStatement.setDate(1,  new java.sql.Date(new SimpleDateFormat("yyyy-MM-dd").parse(partitionToMove).getTime()));
                      break;
                    case TIMESTAMP:
                      preparedStatement.setTimestamp(1,  new java.sql.Timestamp(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").parse(partitionToMove.substring(0,23)).getTime()));
                      break;
                    case TINYINT:
                      preparedStatement.setByte(1,Byte.parseByte(partitionToMove));
                      break;
                    case SMALLINT:
                      preparedStatement.setShort(1,Short.parseShort(partitionToMove));
                      break;
                    case INT:
                      preparedStatement.setInt(1,Integer.parseInt(partitionToMove));
                      break;
                    case BIGINT:
                      preparedStatement.setLong(1,Long.parseLong(partitionToMove));
                      break;
                    case CHAR:
                      preparedStatement.setString(1, partitionToMove);
                      break;
                    default:
                  }//end switch
                  preparedStatement.executeUpdate();
                  System.out.println(String.format("upserted partition: %s with thread %d",partitionToMove,taskNb));
                }//end else
                synchronized (partitionsToMove){
                  try{
                    partitionToMove = partitionsToMove.removeFirst();
                  }catch(NoSuchElementException e){
                    endOfList = true;
                  }
                }//end synchornized
            }//end while
            statement.execute("cqd HIVE_DATA_MOD_CHECK RESET;");
            if (dop != 0) {
            	statement.execute("cqd PARALLEL_NUM_ESPS RESET;");
            	statement.execute("cqd HIVE_PARTITION_INSERT_REPART RESET;");
            	statement.execute("cqd HIVE_PARTITION_AUTO_CREATE RESET;");//we have pre-created partitions
            	statement.execute("cqd HIVE_PARTITION_INSERT_DIST_LOCK RESET;");//should not be needed given above cqd, but just in case 

            }
            if (!type4driver) statement.execute("cqd AS_AFFINITY_VALUE RESET;");
            statement.execute("cqd HBASE_NUM_CACHE_ROWS_MAX RESET;");
            System.out.println(String.format("Done with thread %d",taskNb));
          }catch (Exception e) {
            t.interrupt(); //notify that we have existed abnormally
            // record any exceptions encountered
            success = false;
            System.out.println(String.format("Thread %d, partition %s, Exception:%s",taskNb,partitionToMove,e.toString()));
            logger.error(String.format("Thread %d, partition %s, Exception:%s StackTrace:%s",taskNb,partitionToMove,e.toString(),getStackTrace(e)));
          }finally{
            if (statement != null)
                statement.close();
            if (preparedStatement != null) 
                preparedStatement.close();
            if (conn != null )
                conn.close();
          }
        }catch(Exception ex){}
	  }//end run
    }//end class UpsertIntoPartitionTask
    private static String buildSQLExceptionMessage(Exception ex) {
    	if(!(ex instanceof SQLException)){
    		
    		return ex.toString();
    	}
    	StringBuilder sb = new StringBuilder();
        for (Throwable e : (SQLException) ex) {
            if (e instanceof SQLException) {
                    sb.append(getStackTrace(e))
                      .append('\n')
                      .append("SQLState: ")
                      .append(((SQLException)e).getSQLState())
                      .append("Error Code: ")
                      .append(((SQLException)e).getErrorCode())
                      .append("Message: ")
                      .append(e.getMessage());
                    Throwable t = ex.getCause();
                    while(t != null) {
                        sb.append("Cause: ")
                          .append(t)
                          .append('\n');
                        t = t.getCause();
                    }          
            }else{//not SQLException
            	sb.append("a"+e.toString());
            }
        }
        return sb.toString();
    }
    private static String getStackTrace(Throwable aThrowable) {
        final Writer result = new StringWriter();
        final PrintWriter printWriter = new PrintWriter(result);
        aThrowable.printStackTrace(printWriter);
        return result.toString();
      }
}//end class HotColdTable
