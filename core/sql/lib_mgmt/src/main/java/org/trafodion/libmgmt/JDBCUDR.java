/**********************************************************************
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
**********************************************************************/

/***************************************************
 * A TMUDF that executes a generic JDBC query
 * and returns the result of the one SQL statement
 * in the list that produces results as a table-valued
 * output
 *
 * Invocation (all arguments are strings):
 *
 * select ... from udf(JDBC(
 *    <name of JDBC connection config file>,
 *    <statement_type>,
 *    <optional parameters dependent on statement type> )) ...
 *
 * The first 2 arguments are required. They must be string
 * literals that are available at compile time.
 * The optional arguments described below must also be
 * passed in as literals, so that they are available at
 * compile time.
 *
 * The second parameter is a statement type and it determines
 * the behavior of the UDF.
 *
 * Supported statement types and optional parameters for these
 * types:
 *
 * 'source':
 *
 *    This statement type allows multiple SQL statements as
 *    following arguments. These statements are executed in
 *    a single UDF instance (not in parallel), one after
 *    another. Only one of the statements can return results.
 *    The others can perform setup and cleanup operations,
 *    if necessary (e.g. create table, insert, select,
 *    drop table).
 *    The UDF returns the results of that one statement that
 *    returns rows, if it exists, or it returns no rows.
 *
 *    Arguments for statement type 'source':
 *
 *    'source',
 *    <sql statement 1>
 *    [ , <sql statements 2 ...n> ]
 *
 *    All statements must be passed in as literals, so that
 *    the are available at compile time.
 *
 *    For an example, see file core/sql/regress/udr/TEST002.
 *
 * 'source_parallel_spark':
 *
 *    This statement type executes a single statement in
 *    parallel, using multiple UDF instances. The caller
 *    must parametrize the statement, similar to the way
 *    it is done for the JdbcRDD in Apache Spark, except
 *    that we are using %d or %1$d and %2$d instead of
 *    actual parameters.
 *
 *    Arguments for statement type 'source_parallel_string':
 *
 *    'source_parallel_spark',
 *    <sql statement with 2 embedded parameters>,
 *    <lower_bound>,
 *    <upper_bound>,
 *    <num_partitions>
 *
 *    <lower_bound>, <upper_bound> and <num_partitions>
 *    are integers. At runtime, <num_partitions> instances
 *    of the query will be executed. Each will provide
 *    a non-overlapping range of integers to the query.
 *    For example, lower bound = 1, upper bound = 20,
 *    num partitions = 2 will result in values 1 and 10
 *    for the first query, and values 11 and 20 for the
 *    second.
 *
 *    Example 1 (MySQL, using LIMIT):
 *
 *      'source_parallel_spark',
 *      'select * from t order by a limit %d, 1000',
 *      0,
 *      2999,
 *      3
 *
 *    Example 2 (simple predicate-based partitioning):
 *
 *      'source_parallel_spark',
 *      'select * from t where a between %d and %d',
 *      0,
 *      2999,
 *      3
 *
 * 'source_parallel_limit':
 *
 *    This statement type executes a single statement in
 *    parallel, using multiple UDF instances, like statement
 *    type source_parallel_string. However, the argument
 *    syntax is easier and for the case where we have something
 *    like the "limit offset, count" clause in MySQL.
 *    Here, the format string can use 2 parameters: %1$d is the
 *    0-based first row to return and %2$d is the number of rows
 *    returned per partition.
 *
 *    Arguments for statement type 'source_parallel_limit':
 *
 *    'source_parallel_limit',
 *    <sql statement with 2 embedded parameters>,
 *    <desired_degree_of_parallelism>,
 *    <number_of_rows_per_partition>
 *
 *    Example 3 (MySQL, using LIMIT):
 *
 *      'source_parallel_limit',
 *      'select * from t order by a limit %1$d, %2$d',
 *      3,
 *      1000
 *
 *    Example 4 (PostgreSQL, using LIMIT):
 *
 *      'source_parallel_limit',
 *      'select * from t order by a limit %2$d offset %1$d',
 *      3,
 *      1000
 *
 *    Example 5 (Oracle, using ROWNUM):
 *
 *      'source_parallel_limit',
 *      'select * from (select *, rownum as rnum '
 *                     'from t where rownum <= %1$d+%2$d '
 *                     'order by a) '
 *      'where rnum > %1$d',
 *      3,
 *      1000
 *
 * 'source_parallel_ranges':
 *
 *    This statement type executes a single statement in
 *    parallel, using multiple UDF instances. The caller
 *    must parametrize the statement and must provide the
 *    values of the parameters, e.g. partition names or
 *    numbers or starting row counts. To so that, the SQL
 *    statement must be in a format understandable by the
 *    Java formatter java.util.formatter. If the desired
 *    degree of parallelism is p and n p * n values are
 *    passed as arguments after the SQL statement, then
 *    the statement string must use at most n arguments
 *    when being formatted.
 *
 *    Arguments for statement type 'source_parallel_string':
 *
 *    'source_parallel_ranges',
 *    <desired degree of parallelism>,
 *    <sql statement with n embedded parameters>
 *    [ , <val1> ... <valn> ] ...
 *
 *    The statement must be passed in as a literal, so that
 *    it is available at compile time. The first set of n
 *    parameters must also be provided as literals.
 *
 *    Example 6 (Oracle partition syntax):
 *
 *      'source_parallel_ranges',
 *      3,
 *      'select * from t partition %s',
 *      'part1',
 *      'part2',
 *      'part3'
 *
 *    Example 7 (Oracle index-organized table or Trafodion table):
 *
 *      'source_parallel_ranges',
 *      3,
 *      'select * from t where (t.k1, t.k2) between (%d, ''%s'') and (%d, ''%s'')',
 *      -999999, 'AA',    0, 'AA',
 *      0,       'AB',    0, 'MZ',
 *      0,       'N ', 9999, 'ZZ'
 *
 * 'source_table_mysql':
 *
 *    This statement type reads a single MySQL table and tries to
 *    parallelize the read operation. This type allows push-down of
 *    simple predicates to MySQL and it eliminates unnecessary
 *    columns.

 *    Arguments for statement type 'source_table_mysql':
 *
 *    'source_table_mysql',
 *    <MySQL table name>,
 *    <desired_degree_of_parallelism>
 *
 *    Example 8 (MySQL, single table mode ):
 *
 *      'source_table_mysql',
 *      'mytable',
 *      3
 *
 * 'source_table_oracle':
 *
 *    This statement type reads a single Oracle table and tries to
 *    parallelize the read operation. This type allows push-down of
 *    simple predicates to MySQL and it eliminates unnecessary
 *    columns.

 *    Arguments for statement type 'source_table_oracle':
 *
 *    'source_table_oracle',
 *    <Oracle table name>,
 *    [ , <optional_desired_degree_of_parallelism> ]
 *    [ , <optional explain_table_name> ]
 *
 *    The optional explain table name must be a table that is
 *    suitable for the EXPLAIN PLAN feature in Oracle. The
 *    UDF must have write access to the table. Providing
 *    this will allow the UDF to get a cardinality estimate,
 *    and it is required for degress of parallelism > 1.
 *
 *    Example 9 (Oracle, single table mode):
 *
 *      'source_table_oracle',
 *      'mytable',
 *      3,
 *      'udr_plan_table'
 *
***************************************************/
package org.trafodion.libmgmt;

import java.io.File;
import java.io.FileOutputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
//import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.FsAction;

import org.trafodion.sql.udr.*;
import java.sql.*;
import java.util.Vector;
import java.lang.Math;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.io.PrintStream;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.UUID;
import java.nio.ByteBuffer;

import java.math.BigDecimal;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

public class JDBCUDR extends UDR
{
    // statement types
    enum StmtType {
        UNKNOWN_STMT_TYPE,
        SOURCE,
        SOURCE_PARALLEL_SPARK,
        SOURCE_PARALLEL_LIMIT,
        SOURCE_PARALLEL_RANGES,
        SOURCE_TABLE_MYSQL,
        SOURCE_TABLE_ORACLE
    }

    // known data sources (others are supported as well, but not customized)
    enum DataSource {
        UNKNOWN_DATA_SOURCE,
        MYSQL,
        ORACLE,
        POSTGRESQL,
        TRAFODION
    }

    // class used to connect, both at compile and at runtime
    static class JdbcConnectionInfo
    {
        String driverJar_;
        String driverClassName_;
        String connectionString_;
        String username_;
        String password_;
        boolean debug_;

        Connection conn_;

        public void setJar(String jar)
                                                     { driverJar_ = jar; }
        public void setClass(String className)
                                         { driverClassName_ = className; }
        public void setConnString(String connString)
                                       { connectionString_ = connString; }
        public void setUsername(String userName)
                                                 { username_ = userName; }
        public void setPassword(String password)
                                                 { password_ = password; }
        public void setDebug(boolean debug)            { debug_ = debug; }

        public Connection connect() throws UDRException
        {
          if (conn_ != null)
          {
            return conn_;
          }

          try {
            // code for the older class loader, before the fix for TRAFODION-2534
            Path driverJarPath = Paths.get(driverJar_);
            String trafHome = System.getenv("TRAF_HOME");

            if (trafHome == null || trafHome.length() == 0)
                // running on an older code base
                trafHome = System.getenv("MY_SQROOT");

            Path sandBoxPath = Paths.get(trafHome, "udr", "public", "external_libs");
            URLClassLoader jdbcJarLoader = null;
            URL jarClassPath[] = new URL[1];

            // for security reasons, we sandbox the allowed driver jars
            // into $TRAF_HOME/udr/public/external_libs
            driverJarPath = driverJarPath.normalize();
            if (driverJarPath.isAbsolute())
              {
                if (! driverJarPath.startsWith(sandBoxPath))
                  throw new UDRException(
                    38010,
                    "The jar name of the JDBC driver must be a name relative to %s, got %s",
                    sandBoxPath.toString(),
                    driverJar_);
              }
            else
              driverJarPath = sandBoxPath.resolve(driverJarPath);

            // for security reasons we also reject the Trafodion T2
            // driver (check both class name and URL)
            if (driverClassName_.equals("org.apache.trafodion.jdbc.t2.T2Driver"))
                throw new UDRException(
                    38012,
                    "This UDF does not support the Trafodion T2 driver class %s",
                    driverClassName_);

            if (LmT2Driver.checkURL(connectionString_))
                throw new UDRException(
                    38013,
                    "This UDF does not support the Trafodion T2 driver URL %s",
                    connectionString_);

            // Create a class loader that can access the
            // jar file specified by the caller.
            jarClassPath[0] = driverJarPath.toUri().toURL();
            jdbcJarLoader = new URLClassLoader(
                jarClassPath,
                this.getClass().getClassLoader());

            // go through an intermediary driver, since the DriverManager
            // will not accept classes that are not loaded by the default
            // class loader
            Driver d = (Driver) Class.forName(driverClassName_, true, jdbcJarLoader).newInstance();
            DriverManager.registerDriver(new URLDriver(d));
            // end of code for old class loader. With the fix for TRAFODION-2534,
            // replace the above with the following:
            // Class.forName(driverClassName_);
            // The first parameter (jar name) is no longer needed
            // with the new class loader.
            conn_ = DriverManager.getConnection(connectionString_,
                                                username_,
                                                password_);
            return conn_;
          }
          catch (ClassNotFoundException cnf) {
              String trafHome = System.getenv("TRAF_HOME");

              if (trafHome == null || trafHome.length() == 0)
                  // running on an older code base
                  trafHome = System.getenv("MY_SQROOT");

              throw new UDRException(
                38020,
                "JDBC driver class %s not found. Please make sure the JDBC driver jar is stored in %s. Message: %s",
                driverClassName_,
                trafHome + "/udr/public/external_libs",
                cnf.getMessage());
          }
          catch (SQLException se) {
              throw new UDRException(
                38020,
                "SQL exception during connect. Message: %s",
                se.getMessage());
          }
          catch(Exception e) {
              if (debug_)
                  {
                      //System.out.println("Debug: Exception during connect:");
                      try { e.printStackTrace(System.out); }
                      catch (Exception e2) {}
                  }
              throw new UDRException(
                38020,
                "Exception during connect: %s",
                e.getMessage());
          }
        }

        public Connection getConnection()                 { return conn_; }

        public void disconnect() throws SQLException
        {
            if (conn_ != null)
                conn_.close();
            conn_ = null;
        }
    };

    // list of SQL statements to execute
    static class SQLStatementInfo
    {
        private StmtType stmtType_;

        // remote data store brand, if available
        private DataSource dataSource_;

        // list of SQL statements to execute
        private Vector<String> sqlStrings_;

        // which of the above is the one that
        // produces the table-valued result?
        private int resultStatementIndex_;

        // prepared result-producing statement
        private PreparedStatement resultStatement_;

        // for parallel flavors, number of partitions
        private int numberOfPartitions_;
        private boolean manualPartition_;

        // estimated number of rows (could come from
        // parameter or heuristic or from remote DMBS)
        private long estimatedNumRows_;

        // for Spark flavor only: lower/upper bounds
        private long sparkLowerBound_;
        private long sparkUpperBound_;

        // for flavors that supply parameter values
        private int numFormatParams_;
        private int firstFormatParamNum_;

        // for limit flavor
        private long numLimitRows_;

        // for flavors that read a single table
        private boolean singleTable_;
        private String tableName_;
        private String planTableName_;

        // for debugging
        private boolean debugMode_;


        SQLStatementInfo()
        {
            stmtType_ = StmtType.UNKNOWN_STMT_TYPE;
            dataSource_ = DataSource.UNKNOWN_DATA_SOURCE;
            sqlStrings_ = new Vector<String>();
            resultStatementIndex_ = -1;
            numberOfPartitions_ = 1;
            manualPartition_ = false;
            estimatedNumRows_ = -1;
            sparkLowerBound_ = -1;
            sparkUpperBound_ = -1;
        }

        void setStmtType(StmtType t)
        {
            stmtType_ = t;
        }

        void setDataSource(DataSource s)
        {
            dataSource_ = s;
        }

        void addStatementText(String sqlText)
        {
            sqlStrings_.add(sqlText);
        }

        void addResultProducingStatement(PreparedStatement preparedStmt,
                                         int resultStatementIndex)
        {
            resultStatement_ = preparedStmt;
            resultStatementIndex_ = resultStatementIndex;
        }

        void setNumberOfPartitions(int p)
        {
            numberOfPartitions_ = p;
        }

        void setManualPartitions(boolean b)
        { manualPartition_ = b; }

        void setEstimatedNumRows(long e)
        {
            estimatedNumRows_ = e;
        }

        void setSparkLowerBound(long b)
        {
            sparkLowerBound_ = b;
        }

        void setSparkUpperBound(long b)
        {
            sparkUpperBound_ = b;
        }

        void setNumFormatParams(int p)
        {
            numFormatParams_ = p;
        }

        void setFirstFormatParamNum(int p)
        {
            firstFormatParamNum_ = p;
        }

        void setNumLimitRows(long l)
        {
            numLimitRows_ = l;
        }

        void setTableName(String tn)
        {
            singleTable_ = true;
            tableName_ = tn;
        }

        void setPlanTableName(String tn)
        {
            planTableName_ = tn;
        }

        void setDebugMode(boolean b)
        {
            debugMode_ = b;
        }

        StmtType getStmtType()                       { return stmtType_; }
        DataSource getDataSource()                 { return dataSource_; }
        String getStatementText(int ix)    { return sqlStrings_.get(ix); }
        PreparedStatement getResultStatement(){ return resultStatement_; }
        int getNumStatements()              { return sqlStrings_.size(); }
        int getResultStatementIndex()    { return resultStatementIndex_; }
        int getNumberOfPartitions()        { return numberOfPartitions_; }
        boolean getManualPartitions()        { return manualPartition_; }
        long getEstimatedNumRows()           { return estimatedNumRows_; }
        long getSparkLowerBound()             { return sparkLowerBound_; }
        long getSparkUpperBound()             { return sparkUpperBound_; }
        int getNumFormatParams()              { return numFormatParams_; }
        int getFirstFormatParamNum()      { return firstFormatParamNum_; }
        long getNumLimitRows()                   { return numLimitRows_; }
        boolean getIsSingleTable()                { return singleTable_; }
        String getTableName()                       { return tableName_; }
        String getPlanTableName()               { return planTableName_; }
        boolean getDebugMode()                      { return debugMode_; }

        String formatStatementText(UDRInvocationInfo info,
                                   int partitionNum) throws UDRException {
            return formatStatementText(info,
                                       resultStatementIndex_,
                                       partitionNum);
        }

        // create the SQL statement to send to the remote data source,
        // based on the current compile/execution phase and the
        // parallel instance number at runtime
        String formatStatementText(UDRInvocationInfo info,
                                   int stmtNum,
                                   int partitionNum) throws UDRException {
            switch (stmtType_)
                {
                case SOURCE:
                    return getStatementText(stmtNum);

                case SOURCE_PARALLEL_SPARK:
                    {
                        long rowsPerPartition =
                            (sparkUpperBound_ - sparkLowerBound_ + 1) / numberOfPartitions_;
                        long startValue =
                            sparkLowerBound_ + partitionNum * rowsPerPartition;
                        long endValue =
                            (partitionNum < numberOfPartitions_-1 ?
                             startValue + rowsPerPartition - 1 :
                             sparkUpperBound_);

                        return String.format(getStatementText(stmtNum),
                                             startValue,
                                             endValue);
                    }

                case SOURCE_PARALLEL_LIMIT:
                    {
                        long numRowsPerPartition = getNumLimitRows();
                        long offset = getNumLimitRows() * partitionNum;

                        if (partitionNum == numberOfPartitions_-1)
                            // last partition needs to read all the rows
                            // (well, up to reason, which we define to be
                            // just 1 shy of a trillion here)
                            numRowsPerPartition = 999999999999L;

                        return String.format(getStatementText(stmtNum),
                                             offset,
                                             numRowsPerPartition);
                    }

                case SOURCE_PARALLEL_RANGES:
                    {
                        int numFormatItems = numFormatParams_ / numberOfPartitions_;
                        Object[] objs = new Object[numFormatItems];
                        int startFormatParamNum = partitionNum * numFormatItems;
                        int objIx = 0;

                        for (int p = startFormatParamNum;
                             p < startFormatParamNum + numFormatItems;
                             p++)
                            {
                                Object f;

                                switch (info.par().getType(p).getSQLTypeSubClass())
                                    {
                                    case FIXED_CHAR_TYPE:
                                    case VAR_CHAR_TYPE:
                                    case DATE_TYPE:
                                    case TIME_TYPE:
                                    case TIMESTAMP_TYPE:
                                    //case BOOLEAN_SUB_CLASS: << enable this in 2.3
                                        f = info.par().getString(p);
                                        break;

                                    case EXACT_NUMERIC_TYPE:
                                    case YEAR_MONTH_INTERVAL_TYPE:
                                    case DAY_SECOND_INTERVAL_TYPE:
                                        f = new Long(info.par().getLong(p));

                                    case APPROXIMATE_NUMERIC_TYPE:
                                        f = new Double(info.par().getDouble(p));
                                        break;

                                    default:
                                        throw new UDRException(
                                          38000,
                                          "Unsupported data type in argument %d for UDF %s: Not supported for formatting",
                                          p,
                                          info.getUDRName());
                                    }

                                objs[objIx++] = f;
                            }

                        return String.format(getStatementText(stmtNum), objs);
                    } // case SOURCE_PARALLEL_RANGES

                case SOURCE_TABLE_MYSQL:
                case SOURCE_TABLE_ORACLE:
                    if (info.getCallPhase() == UDRInvocationInfo.CallPhase.COMPILER_INITIAL_CALL)
                        // in the initial phase we select all columns and apply no where predicates
                        return "select * from " + getTableName();
                    else
                        return formatSelectionAndProjection(info, partitionNum);

                } // case stmtType_

            throw new UDRException(38000, "Internal error");
        }

        // create a select statement for a single table with only the
        // needed columns and with the pushed-down predicates
        private String formatSelectionAndProjection(UDRInvocationInfo info,
                                                    int partitionNum)
            throws UDRException
        {
            StringBuilder sql = new StringBuilder(200);
            boolean analyzePredicates =
                (info.getCallPhase() == UDRInvocationInfo.CallPhase.COMPILER_DATAFLOW_CALL);
            int numOutCols = info.out().getNumColumns();
            int numUsedCols = 0;
            int numPushedPreds = 0;
            boolean useLimitClause =
                (numberOfPartitions_ > 1 && estimatedNumRows_ >= numberOfPartitions_);
            boolean useOracleRownum =
                (useLimitClause && getDataSource() == DataSource.ORACLE);

            // in this method, we'll build an SQL statement of this form:
            // "select col1, ... coln from t where pred1 and ... predn"
            // this method is meant to be called from the
            // describeDataflowAndPredicates() method

            sql.append("select ");

            // loop over output columns and eliminate any that are not needed
            for (int oc=0; oc<numOutCols; oc++)
                {
                    ColumnInfo colInfo = info.out().getColumn(oc);

                    switch (colInfo.getUsage())
                        {
                        case USED:
                            // this column is used, append its name to the SELECT stmt
                            if (numUsedCols > 0)
                                sql.append(", ");
                            sql.append(colInfo.getColName());
                            numUsedCols++;
                            break;

                        case NOT_USED:
                            // this column is not needed, so don't get it through JDBC
                            colInfo.setUsage(ColumnInfo.ColumnUseCode.NOT_PRODUCED);
                            break;

                        case NOT_PRODUCED:
                            break;

                        default:
                            throw new UDRException(
                                38000,
                                "Internal error, invalid column use code for column %d",
                                oc);
                        }
                }

            if (numUsedCols == 0)
                // no columns needed, e.g. for select count(*), add a dummy
                sql.append("1");

            if (useOracleRownum)
                sql.append(", rownum as r___num");

            sql.append(" from " + getTableName());

            // Walk through predicates and find additional
            // ones to push down or to evaluate locally
            for (int p=0; p<info.getNumPredicates(); p++)
                {
                    if (info.isAComparisonPredicate(p))
                        {
                            // For demo purposes, accept predicates
                            // of the form "session_id < const" to
                            // be evaluated in the UDF.
                            ComparisonPredicateInfo cpi =
                                info.getComparisonPredicate(p);

                            if (cpi.getEvaluationCode() == PredicateInfo.EvaluationCode.EVALUATE_IN_UDF ||
                                analyzePredicates && cpi.hasAConstantValue())
                                {
                                    if (analyzePredicates)
                                        info.setPredicateEvaluationCode(
                                            p,
                                            PredicateInfo.EvaluationCode.EVALUATE_IN_UDF);

                                    if (numPushedPreds == 0)
                                        sql.append(" where ");
                                    else
                                        sql.append(" and ");

                                    // need to make ComparisonPredicate.toString() public
                                    // for now just clone the code
                                    // sql.append(cpi.toString(info.out()));

                                    sql.append(info.out().getColumn(cpi.getColumnNumber()).getColName());

                                    switch (cpi.getOperator())
                                        {
                                        case EQUAL:
                                            sql.append(" = ");
                                            break;
                                        case NOT_EQUAL:
                                            sql.append(" <> ");
                                            break;
                                        case LESS:
                                            sql.append(" < ");
                                            break;
                                        case LESS_EQUAL:
                                            sql.append(" <= ");
                                            break;
                                        case GREATER:
                                            sql.append(" > ");
                                            break;
                                        case GREATER_EQUAL:
                                            sql.append(" >= ");
                                            break;
                                        case IN:
                                            sql.append(" in ");
                                            break;
                                        case NOT_IN:
                                            sql.append(" not in ");
                                            break;
                                        default:
                                            throw new UDRException(
                                                38000,
                                                "Unsupported predicate operator for column %s",
                                                info.out().getColumn(cpi.getColumnNumber()).getColName());
                                        } // switch

                                    sql.append(cpi.getConstValue());
                                    numPushedPreds++;
                                } // column op constant predicate
                        } // a comparison predicate
                } // loop over predicates

            // if we have multiple partitions and also a row count estimate, then
            // append a LIMIT offset, count clause
            if (useLimitClause)
                {
                    long numRowsPerPartition = estimatedNumRows_ / numberOfPartitions_;
                    long offset = numRowsPerPartition * partitionNum;

                    if (partitionNum == numberOfPartitions_-1)
                        // last partition needs to read all the rows
                        // (well, up to reason, which we define to be
                        // just 1 shy of a trillion here)
                        numRowsPerPartition = 999999999999L;

                    switch (getDataSource())
                        {
                        case MYSQL:
                            sql.append(" limit " + offset + ", " + numRowsPerPartition);
                            break;
                        case ORACLE:
                            {
                                // change "select xx, rownum as r___num from yy where zz" to
                                // "select xx
                                //  from  (select xx, rownum as r___num from yy where zz and rownum <= aa)
                                //  where r___num > bb"
                                StringBuilder outerSel = new StringBuilder(200);
                                int numSelCols = 0;

                                outerSel.append("select ");

                                for (int oc2=0; oc2<numOutCols; oc2++)
                                    {
                                        ColumnInfo colInfo = info.out().getColumn(oc2);

                                        if (colInfo.getUsage() == ColumnInfo.ColumnUseCode.USED)
                                        {
                                            if (numSelCols > 0)
                                                outerSel.append(", ");
                                            numSelCols++;
                                            outerSel.append(colInfo.getColName());
                                        }
                                    }

                                outerSel.append(" from (");
                                sql.insert(0, outerSel);

                                if (numPushedPreds == 0)
                                    sql.append(" where");
                                else
                                    sql.append(" and");
                                sql.append(" rownum <= " +
                                           (offset + numRowsPerPartition) +
                                           ") where r___num > " + offset);
                            }
                            break;
                        default:
                            throw new UDRException(
                                38900,
                                "Internal error, invalid data source for single table");
                        }

                }

            //System.out.println(sql);
            return sql.toString();
        }

    }; // end class SQLStatementInfo

    // Define data that gets passed between compiler phases
    static class JdbcUDRCompileTimeData extends UDRWriterCompileTimeData
    {
        JdbcConnectionInfo jci_;
        SQLStatementInfo sqi_;

        JdbcUDRCompileTimeData()
        {
            jci_ = new JdbcConnectionInfo();
            sqi_ = new SQLStatementInfo();
        }
    };

    // Define data that gets passed from the compiler to the runtime instances
    static class JdbcUDRPlanData
    {
        int dop_;
        long estNumRows_;
        String formattedStatement_;

        // default constructor, use this at compile time
        JdbcUDRPlanData()
        {
            init();
        }

        // constructor used at runtime, deserialize info from plan
        JdbcUDRPlanData(UDRPlanInfo plan) throws UDRException
        {
            byte[] planData = plan.getPlanData();

            if (planData == null || planData.length == 0)
                init();
            else
                {
                    ByteBuffer bb = ByteBuffer.wrap(planData);
                    int stmtLen;
                    byte[] stmtArray;

                    dop_ = bb.getInt();
                    estNumRows_ = bb.getLong();
                    stmtLen = bb.getInt();

                    if (stmtLen > 0)
                        {
                            stmtArray = new byte[stmtLen];
                            bb.get(stmtArray);
                            try {
                                formattedStatement_ = new String(stmtArray, "UTF8");
                            }
                            catch (Exception e) {
                                throw new UDRException(
                                                       38900,
                                                       "Internal error converting serialized statement from UTF8");
                            }
                        }
                }
        }

        void init()
        {
            dop_ = -1;
            estNumRows_ = -1;
        }

        // add this object to the plan, so it gets sent to all the
        // parallel instances at runtime
        void addToPlan(UDRPlanInfo plan) throws UDRException
        {
            int stmtLen = 0;
            byte[] stmtAsBytes = null;

            if (formattedStatement_ != null)
                {
                    try {
                        stmtAsBytes = formattedStatement_.getBytes("UTF8");
                    }
                    catch (Exception e) {
                        throw new UDRException(
                            38900,
                            "could not convert SQL statement to UTF8");
                    }
                    stmtLen = stmtAsBytes.length;
                }

            // hard-code sizes of dop_, estNumRows_, stmtLen here as 4+8+4
            ByteBuffer bb = ByteBuffer.allocate(4+8+4+stmtLen);

            bb.putInt(dop_);
            bb.putLong(estNumRows_);
            bb.putInt(stmtLen);
            if (stmtLen > 0)
                bb.put(stmtAsBytes);

            plan.addPlanData(bb.array());
        }

    };

    // Custom driver class, workaround for issues in JDBC when using a
    // customer class loader, as Trafodion UDRs do
    static class URLDriver implements Driver {
        private Driver driver_;
        URLDriver(Driver d) { driver_ = d; }
        public boolean acceptsURL(String u) throws SQLException {
                return driver_.acceptsURL(u);
        }
        public Connection connect(String u, Properties p) throws SQLException {
                return driver_.connect(u, p);
        }
        public int getMajorVersion() {
                return driver_.getMajorVersion();
        }
        public int getMinorVersion() {
                return driver_.getMinorVersion();
        }
        public DriverPropertyInfo[] getPropertyInfo(String u, Properties p) throws SQLException {
                return driver_.getPropertyInfo(u, p);
        }
        public boolean jdbcCompliant() {
                return driver_.jdbcCompliant();
        }
        public Logger getParentLogger() throws SQLFeatureNotSupportedException {
                return driver_.getParentLogger();
        }
    }

    JdbcConnectionInfo getConnectionInfo(UDRInvocationInfo info) throws UDRException
    {
        return ((JdbcUDRCompileTimeData) info.getUDRWriterCompileTimeData()).jci_;
    }

    SQLStatementInfo getSQLStatementInfo(UDRInvocationInfo info) throws UDRException
    {
        return ((JdbcUDRCompileTimeData) info.getUDRWriterCompileTimeData()).sqi_;
    }


    // default constructor
    public JDBCUDR()
    {}

    // a method to process the input parameters, this is
    // used both at compile time and at runtime
    private void handleInputParams(UDRInvocationInfo info,
                                   JdbcConnectionInfo jci,
                                   SQLStatementInfo sqi)
                                             throws UDRException
    {
        int numInParams = info.par().getNumColumns();
        boolean isCompileTime = info.isCompileTime();

        // Right now we don't support table inputs
        if (isCompileTime && info.getNumTableInputs() != 0)
            throw new UDRException(
              38300,
              "%s must be called without table-valued inputs",
              info.getUDRName());

        if (numInParams < 3)
            throw new UDRException(
              38310,
              "Expecting at least 3 parameters for %s UDR",
              info.getUDRName());

        // get JDBC connection information from HDFS config file
        {
            String jar        = null;
            String class_name = null;
            String url        = null;
            String user_name  = null;
            String user_pwd   = null;

            String HdfsConfDir = "/user/trafodion/udr/jdbc/";
            org.apache.hadoop.fs.Path confDir = null;
            org.apache.hadoop.fs.Path confPath = null;

            try {
                String fileName = info.par().getString(0);

                confDir    = new org.apache.hadoop.fs.Path(HdfsConfDir);
                confPath   = new org.apache.hadoop.fs.Path(HdfsConfDir, fileName);

                Configuration conf = new Configuration(true);
                FileSystem  fs = FileSystem.get(conf);

                if (false == fs.exists(confDir))
                    fs.mkdirs(confDir, new FsPermission(FsAction.ALL,
                                                        FsAction.NONE,
                                                        FsAction.NONE));

                FSDataInputStream confFileStream = fs.open(confPath);
                BufferedReader reader = new BufferedReader(new InputStreamReader(confFileStream));

                jar        = reader.readLine();
                class_name = reader.readLine();
                url        = reader.readLine();
                user_name  = JDBCUDRUtil.decryptByAes(reader.readLine());
                user_pwd   = JDBCUDRUtil.decryptByAes(reader.readLine());

                reader.close();
                confFileStream.close();
            }
            catch (Exception e) {
                throw new UDRException(38973, "Error read JDBC_PARALLEL config file from hdfs file %s, reason: %s",
                                       (confPath != null) ? confPath.toString() : "null",
                                       e.getMessage());
            }

            // try to detect some data sources that would allow us to use
            // DBMS-specific interfaces such as EXPLAIN
            if (class_name.startsWith("com.mysql.jdbc."))
                sqi.setDataSource(DataSource.MYSQL);
            else if (class_name.startsWith("oracle.jdbc.driver."))
                sqi.setDataSource(DataSource.ORACLE);
            else if (class_name.startsWith("org.postgresql."))
                sqi.setDataSource(DataSource.POSTGRESQL);
            else if (class_name.startsWith("org.trafodion.jdbc."))
                sqi.setDataSource(DataSource.TRAFODION);

            jci.setJar(jar);
            jci.setClass(class_name);
            jci.setConnString(url);
            jci.setUsername(user_name);
            jci.setPassword(user_pwd);
        }

        // loop over scalar input parameters
        for (int p=0; p<numInParams; p++)
            {
                if (isCompileTime &&
                    ! info.par().isAvailable(p))
                    throw new UDRException(
                      38320,
                      "Parameter %d of %s must be a compile time constant",
                      p+1,
                      info.getUDRName());

                String paramValue = info.par().getString(p);

                switch (p)
                    {
                    case 0:
                        break;

                    case 1:
                        {
                            // statement type
                            int minNumParams;

                            // debugging: add "dbg_" to the statement mode, and the
                            // UDF will return the statement text and other info instead
                            // of the actual data
                            if (paramValue.startsWith("dbg_"))
                                {
                                    sqi.setDebugMode(true);
                                    paramValue = paramValue.substring(4);
                                }
                            if (paramValue.compareToIgnoreCase("source") == 0)
                                {
                                    sqi.setStmtType(StmtType.SOURCE);
                                    // need at least one more parameter
                                    minNumParams = p+2;
                                }
                            else if (paramValue.compareToIgnoreCase("source_parallel_spark") == 0)
                                {
                                    sqi.setStmtType(StmtType.SOURCE_PARALLEL_SPARK);
                                    // need four more parameters
                                    minNumParams = p+5;
                                }
                            else if (paramValue.compareToIgnoreCase("source_parallel_ranges") == 0)
                                {
                                    sqi.setStmtType(StmtType.SOURCE_PARALLEL_RANGES);
                                    // need at least two more parameters
                                    minNumParams = p+3;
                                }
                            else if (paramValue.compareToIgnoreCase("source_parallel_limit") == 0)
                                {
                                    sqi.setStmtType(StmtType.SOURCE_PARALLEL_LIMIT);
                                    // need at least three more parameters
                                    minNumParams = p+4;
                                }
                            else if (paramValue.compareToIgnoreCase("source_table_mysql") == 0)
                                {
                                    sqi.setStmtType(StmtType.SOURCE_TABLE_MYSQL);
                                    sqi.setDataSource(DataSource.MYSQL);
                                    // need at least two more parameters
                                    minNumParams = p+3;
                                }
                            else if (paramValue.compareToIgnoreCase("source_table_oracle") == 0)
                                {
                                    sqi.setStmtType(StmtType.SOURCE_TABLE_ORACLE);
                                    sqi.setDataSource(DataSource.ORACLE);
                                    // need at least two more parameters
                                    minNumParams = p+1;
                                }
                            else
                                throw new UDRException(
                                  38330,
                                  "Statement type %s not supported, see parameter 6 of %s",
                                  paramValue,
                                  info.getUDRName());

                            if (numInParams < minNumParams)
                                throw new UDRException(
                                  38330,
                                  "Need at least %d parameters, got %d for UDF %s with statement type %s",
                                  minNumParams,
                                  numInParams,
                                  info.getUDRName(),
                                  paramValue);
                        }
                        break;

                    default:

                        // parameters 6 and above depend on the statement type
                        switch (sqi.getStmtType())
                            {
                            case SOURCE:
                                // SQL statement (there could be multiple)
                                sqi.addStatementText(paramValue);
                                break;

                            case SOURCE_PARALLEL_SPARK:
                                // 4 more parameters, see comment above
                                if (p == 2)
                                    {
                                        // single SQL statement
                                        sqi.addStatementText(paramValue);

                                        // remaining parameters
                                        if (numInParams != 4)
                                            throw new UDRException(
                                              38330,
                                              "Expecting 4 input parameters for UDF %s and statement type SOURCE_PARALLEL_SPARK, got %d",
                                              info.getUDRName(),
                                              numInParams);
                                    }
                                else
                                    {
                                        switch (p)
                                            {
                                            case 3:
                                                //sqi.setSparkLowerBound(info.par().getLong(p));
                                                sqi.setSparkLowerBound(Long.parseLong(info.par().getString(p)));
                                                break;
                                            case 4:
                                                //sqi.setSparkUpperBound(info.par().getLong(p));
                                                sqi.setSparkUpperBound(Long.parseLong(info.par().getString(p)));
                                                break;
                                            case 5:
                                                //sqi.setNumberOfPartitions(info.par().getInt(p));
                                                sqi.setNumberOfPartitions(Integer.parseInt(info.par().getString(p)));
                                                break;
                                            }
                                    }
                                break;

                            case SOURCE_PARALLEL_RANGES:
                                // 2 + n*p parameters, see comment above
                                if (p == 2)
                                    {
                                        // number of partitions
                                        int np = info.par().getInt(p);
                                        int numValues = numInParams - (p+2);

                                        sqi.setNumberOfPartitions(np);
                                        sqi.setFirstFormatParamNum(p+2);
                                        sqi.setNumFormatParams(numValues);

                                        if (np < 1)
                                            throw new UDRException(
                                              38330,
                                              "The desired degree of parallelism (argument 7) must be at least 1");

                                        // check number of parameters
                                        if (np == 1)
                                            {
                                                if (numValues > 0)
                                                    throw new UDRException(
                                                      38330,
                                                      "No partition values should be specified for statement type SOURCE_PARALLEL_RANGES when the desired degree of parallelism (argument 7) is 1");
                                            }
                                        else
                                            {
                                                if (numValues < np)
                                                    throw new UDRException(
                                                      38330,
                                                      "The desired degree of parallelism is %d, expecting n times %d ffollowing parameter values where n is the number of formatting arguments used in the query. Got only %d",
                                                      np,
                                                      np,
                                                      numValues);
                                            }
                                    }
                                else if (p == 3)
                                    // the single SQL statement
                                    sqi.addStatementText(paramValue);
                                break;

                            case SOURCE_PARALLEL_LIMIT:
                                // 3 more parameters, see comment above
                                switch (p)
                                    {
                                    case 2:
                                        // single SQL statement
                                        sqi.addStatementText(paramValue);

                                        // remaining parameters
                                        if (numInParams != 3)
                                            throw new UDRException(
                                              38330,
                                              "Expecting 9 input parameters for UDF %s and statement type SOURCE_PARALLEL_LIMIT, got %d",
                                              info.getUDRName(),
                                              numInParams);
                                        break;
                                    case 3:
                                        //sqi.setNumberOfPartitions(info.par().getInt(p));
                                        sqi.setNumberOfPartitions(Integer.parseInt(info.par().getString(p)));
                                        break;
                                    case 4:
                                        //sqi.setNumLimitRows(info.par().getLong(p));
                                        sqi.setNumLimitRows(Long.parseLong(info.par().getString(p)));
                                        break;
                                    }
                                break;

                            case SOURCE_TABLE_MYSQL:
                                // 2 more parameters, see comment above
                                switch (p)
                                    {
                                    case 2:
                                        // single SQL statement
                                        sqi.setTableName(paramValue);
                                        sqi.addStatementText("select * from " + paramValue);

                                        // remaining parameters
                                        if (numInParams != 2)
                                            throw new UDRException(
                                              38330,
                                              "Expecting 8 input parameters for UDF %s and statement type SOURCE_TABLE_MYSQL, got %d",
                                              info.getUDRName(),
                                              numInParams);
                                        break;
                                    case 3:
                                        //sqi.setNumberOfPartitions(info.par().getInt(p));
                                        sqi.setNumberOfPartitions(Integer.parseInt(info.par().getString(p)));
                                        break;
                                    }
                                break;

                            case SOURCE_TABLE_ORACLE:
                                // 2 or 3 more parameters, see comment above
                                switch (p)
                                    {
                                    case 2:
                                        // single SQL statement
                                        sqi.setTableName(paramValue);
                                        sqi.addStatementText("select * from " + paramValue);
                                        sqi.setNumberOfPartitions(1); // default value

                                        // remaining parameters
                                        if (numInParams < 1 || numInParams > 5)
                                            throw new UDRException(
                                              38330,
                                              "Expecting 3, 4 or 5 input parameters for UDF %s and statement type SOURCE_TABLE_ORACLE, got %d",
                                              info.getUDRName(),
                                              numInParams);
                                        sqi.setPlanTableName("udr_plan_table");

                                        // auto decide paritions based on cardinality
                                        preDescribeStatistics(info, jci, sqi);
                                        break;
                                    case 3:
                                        //sqi.setNumberOfPartitions(info.par().getInt(p));
                                        sqi.setNumberOfPartitions(Integer.parseInt(info.par().getString(p)));
                                        sqi.setManualPartitions(true);
                                        if (sqi.getNumberOfPartitions() != 1 &&
                                            numInParams < 3)
                                            throw new UDRException(
                                              38330,
                                              "When choosing parallel reads from an Oracle table, the 9th parameter specifying the Oracle plan table is required");
                                        break;
                                    case 4:
                                        sqi.setPlanTableName(info.par().getString(p));
                                        break;
                                    }
                                break;

                            default:
                                throw new UDRException(38330, "Internal error");
                            }
                        break;
                    }

                if (isCompileTime)
                    // add the actual parameter as a formal parameter
                    // (the formal parameter list is initially empty)
                    info.addFormalParameter(info.par().getColumn(p));
            }

        jci.setDebug(info.getDebugFlags() != 0);

        // Prepare each provided statement. We will verify that
        // only one of these statements produces result rows,
        // which will become our table-valued output.
        int numSQLStatements = sqi.getNumStatements();

        if (isCompileTime)
        {
            // walk through all statements, check whether they are
            // valid by preparing them, and determine which one is
            // the one that generates a result set
            String currentStmtText = "";
            try
            {
                jci.connect();

                for (int s=0; s<numSQLStatements; s++)
                {
                    currentStmtText = sqi.formatStatementText(info, s, 0);
                    // System.out.printf("Statement to prepare: %s\n", currentStmtText);
                    PreparedStatement preparedStmt =
                            jci.getConnection().prepareStatement(currentStmtText);
                    // if (preparedStmt != null)
                    //    System.out.printf("Prepare was successful\n");
                    ParameterMetaData pmd = preparedStmt.getParameterMetaData();
                    if (pmd != null && pmd.getParameterCount() != 0)
                        throw new UDRException(
                                38360,
                                "Statement %s requires %d input parameters, which is not supported",
                                currentStmtText, pmd.getParameterCount());
                    ResultSetMetaData desc = preparedStmt.getMetaData();

                    int numResultCols = desc.getColumnCount();
                    // System.out.printf("Number of output columns: %d", numResultCols);

                    if (numResultCols > 0)
                    {
                        if (sqi.getResultStatementIndex() >= 0)
                            throw new UDRException(
                                    38370,
                                    "More than one of the statements provided produce output, this is not supported (%d and %d)",
                                    sqi.getResultStatementIndex()+1,
                                    s+1);

                        // we found the statement that is producing the result
                        sqi.addResultProducingStatement(preparedStmt, s);

                        // now add the output columns
                        if (!sqi.getDebugMode() || sqi.getIsSingleTable())
                            for (int c=0; c<numResultCols; c++)
                                {
                                    String colName = desc.getColumnLabel(c+1);
                                    TypeInfo udrType = getUDRTypeFromJDBCType(desc, c+1);

                                    if (sqi.getDataSource() == DataSource.MYSQL ||
                                        sqi.getDataSource() == DataSource.POSTGRESQL)
                                        {
                                            // MySQL and PostgreSQL store their identifiers
                                            // in all lower case format by default. Convert
                                            // this to all upper-case for Trafodion.
                                            if (colName.matches("[a-z0-9_]*"))
                                                colName = colName.toUpperCase();
                                        }

                                    info.out().addColumn(new ColumnInfo(colName, udrType));
                                }
                        else
                            // debug mode for non-single table returns just one char column
                            // (when using this with a single-table mode, make sure the
                            // first column in the table is a character column big enough
                            // to contain the statement text)
                            info.out().addCharColumn("DEBUG_INFO", 32000, false);

                    } // statement produces results
                } // loop over statements
                jci.disconnect();
            }
            catch (SQLException e)
            {
                throw new UDRException(
                        38380,
                        "SQL Exception when preparing SQL statement %s. Exception text: %s",
                        currentStmtText, e.getMessage());
            }
        }
    }

    TypeInfo getUDRTypeFromJDBCType(ResultSetMetaData desc,
                                    int colNumOneBased) throws UDRException
    {
        TypeInfo result;

        final int maxLength = 100000;

        int colJDBCType;

        // the ingredients to make a UDR type and their default values
        TypeInfo.SQLTypeCode      sqlType      = TypeInfo.SQLTypeCode.UNDEFINED_SQL_TYPE;
        int                       length       = 0;
        boolean                   nullable     = false;
        int                       scale        = 0;
        TypeInfo.SQLCharsetCode   charset      = TypeInfo.SQLCharsetCode.CHARSET_UTF8;
        TypeInfo.SQLIntervalCode  intervalCode = TypeInfo.SQLIntervalCode.UNDEFINED_INTERVAL_CODE;
        int                       precision    = 0;
        TypeInfo.SQLCollationCode collation    = TypeInfo.SQLCollationCode.SYSTEM_COLLATION;

        try {
            colJDBCType = desc.getColumnType(colNumOneBased);
            nullable = (desc.isNullable(colNumOneBased) != ResultSetMetaData.columnNoNulls);

            // map the JDBC type to a Trafodion UDR parameter type
            //System.out.println("Type name = "+desc.getColumnTypeName(colNumOneBased)+
            //      "\n colJDBCType = "+colJDBCType);

            switch (colJDBCType)
            {
            case java.sql.Types.SMALLINT:
            case java.sql.Types.TINYINT:
            case java.sql.Types.BOOLEAN:
                if (desc.isSigned(colNumOneBased))
                    sqlType = TypeInfo.SQLTypeCode.SMALLINT;
                else
                    sqlType = TypeInfo.SQLTypeCode.SMALLINT_UNSIGNED;
                break;

            case java.sql.Types.INTEGER:
                if (desc.isSigned(colNumOneBased))
                    sqlType = TypeInfo.SQLTypeCode.INT;
                else
                    sqlType = TypeInfo.SQLTypeCode.INT_UNSIGNED;
                    /*follow 5 lines is added by me*/
                    length  = Math.min(desc.getPrecision(colNumOneBased), maxLength);
                    //System.out.println(" sqlType = " + sqlType +
                    //   "\n length = " + length +
                    //   "\n precision = " + desc.getPrecision(colNumOneBased) +
                    //   "\n scale = " + desc.getScale(colNumOneBased));
                break;

            case java.sql.Types.BIGINT:
                sqlType = TypeInfo.SQLTypeCode.LARGEINT;
                break;

            case java.sql.Types.DECIMAL:
            case java.sql.Types.NUMERIC:
                if (desc.isSigned(colNumOneBased))
                    sqlType = TypeInfo.SQLTypeCode.NUMERIC;
                else
                    sqlType = TypeInfo.SQLTypeCode.NUMERIC_UNSIGNED;

                precision = desc.getPrecision(colNumOneBased);
                scale = desc.getScale(colNumOneBased);

                if (scale == -127) // oracle Float type
                {
                    sqlType = TypeInfo.SQLTypeCode.DOUBLE_PRECISION;
                    break;
                }
                // if scale is 0, then use Largeint to avoid overflow of INT from oracle
                //else if (scale == 0)
                //{
                //    sqlType = TypeInfo.SQLTypeCode.LARGEINT;
                //    break;
                //}

                // limit scale and precision for now
                if (precision > 18 || precision <= 0)
                    precision = 18;
                if (scale > precision - 1 || scale < 0)
                    scale = precision/2;
                break;

            case java.sql.Types.REAL:
                sqlType = TypeInfo.SQLTypeCode.REAL;
                break;

            case java.sql.Types.DOUBLE:
            case java.sql.Types.FLOAT:
                sqlType = TypeInfo.SQLTypeCode.DOUBLE_PRECISION;
                break;

            case java.sql.Types.CHAR:
            case java.sql.Types.NCHAR:
                sqlType = TypeInfo.SQLTypeCode.CHAR;
                length  = Math.min(desc.getPrecision(colNumOneBased) * 4, maxLength);
                charset = TypeInfo.SQLCharsetCode.CHARSET_UTF8;
                break;

            case java.sql.Types.VARCHAR:
            case java.sql.Types.NVARCHAR:
                sqlType = TypeInfo.SQLTypeCode.VARCHAR;
                length  = Math.min(desc.getPrecision(colNumOneBased) * 4, maxLength);
                charset = TypeInfo.SQLCharsetCode.CHARSET_UTF8;
                break;

            case java.sql.Types.DATE:
                sqlType = TypeInfo.SQLTypeCode.DATE;
                break;

            case java.sql.Types.TIME:
                sqlType = TypeInfo.SQLTypeCode.TIME;
                break;

            case java.sql.Types.TIMESTAMP:
                sqlType = TypeInfo.SQLTypeCode.TIMESTAMP;
                /* ISSUE09: need right scale for TIMESTAMP type */
                scale = desc.getScale(colNumOneBased);
                break;

                // BLOB - not supported yet, map to varchar
                // case java.sql.Types.BLOB:
                // sqlType = TypeInfo.SQLTypeCode.BLOB;
                // break;

                // CLOB - not supported yet, map to varchar
                // case java.sql.Types.CLOB:
                // sqlType = TypeInfo.SQLTypeCode.CLOB;
                // break;

            case java.sql.Types.ARRAY:
            case java.sql.Types.BINARY:
            case java.sql.Types.BIT:
            case java.sql.Types.DATALINK:
            case java.sql.Types.DISTINCT:
            case java.sql.Types.JAVA_OBJECT:
            case java.sql.Types.LONGVARBINARY:
            case java.sql.Types.NULL:
            case java.sql.Types.OTHER:
            case java.sql.Types.REF:
            case java.sql.Types.STRUCT:
            case java.sql.Types.VARBINARY:
                // these types produce a binary result, represented
                // as varchar(n) character set iso88591
                sqlType = TypeInfo.SQLTypeCode.VARCHAR;
                // test oracle data type raw because RAW type
                if (desc.getColumnTypeName(colNumOneBased).equals("RAW")) {
                    length = Math.min(desc.getColumnDisplaySize(colNumOneBased),maxLength)*2;
                }else {
                    length  = Math.min(desc.getPrecision(colNumOneBased), maxLength);
                }
                charset = TypeInfo.SQLCharsetCode.CHARSET_ISO88591;
                break;

            case java.sql.Types.LONGVARCHAR:
            case java.sql.Types.LONGNVARCHAR:
            case java.sql.Types.ROWID:
            case java.sql.Types.SQLXML:
                // these types produce a varchar(n) character set utf8 result
                sqlType = TypeInfo.SQLTypeCode.VARCHAR;
                length  = Math.min(desc.getPrecision(colNumOneBased), maxLength);
                charset = TypeInfo.SQLCharsetCode.CHARSET_UTF8;
                break;
            case java.sql.Types.BLOB:
            case java.sql.Types.CLOB:
            case java.sql.Types.NCLOB:

                sqlType = TypeInfo.SQLTypeCode.VARCHAR;
                length = Math.min(desc.getColumnDisplaySize(colNumOneBased),maxLength)*2;
                charset = TypeInfo.SQLCharsetCode.CHARSET_UTF8;
                break;
            /*case 100://oracle data type BINARY_FLOAT
                sqlType = TypeInfo.SQLTypeCode.VARCHAR;
                length  = 7+Math.min(desc.getColumnDisplaySize(colNumOneBased), maxLength);
                charset = TypeInfo.SQLCharsetCode.CHARSET_ISO88591;

                break;

            case 101://oracle data type BINARY_DOUBLE
                sqlType = TypeInfo.SQLTypeCode.VARCHAR;
                length  = 12+Math.min(desc.getColumnDisplaySize(colNumOneBased), maxLength);
                charset = TypeInfo.SQLCharsetCode.CHARSET_ISO88591;
                break;

            case -103://oracle data type INTERVAL DAY TO SECOND
            case -104://oracle data type INTERVAL YEAR TO MONTH
                sqlType = TypeInfo.SQLTypeCode.VARCHAR;
                length  = 10+Math.min(desc.getPrecision(colNumOneBased), maxLength)+desc.getScale(colNumOneBased);
                charset = TypeInfo.SQLCharsetCode.CHARSET_ISO88591;

                break;*/
            }
        } catch (SQLException e) {
            throw new UDRException(
                    38500,
                    "Error determinging the type of output column %d: ",
                    colNumOneBased,
                    e.getMessage());
        }

        result = new TypeInfo(
                sqlType,
                length,
                nullable,
                scale,
                charset,
                intervalCode,
                precision,
                collation);

        return result;
    }

    void preDescribeStatistics(UDRInvocationInfo info, JdbcConnectionInfo jci, SQLStatementInfo sqi)
        throws UDRException
    {
        long numEstimatedRows = 100; // default estimate for non-parallel queries
        sqi.setEstimatedNumRows(numEstimatedRows);

        //if (sqi.getNumberOfPartitions() > 0)
        //    // for now just use a large number, to ensure that
        //    // we get a parallel plan
        //    numEstimatedRows = 1000000;

        if (sqi.getIsSingleTable())
            {
                // get a compiler estimate for the rowcount (done for
                // single table only at this point, could expand to
                // other flavors later)
                switch (sqi.getDataSource())
                    {
                    case MYSQL:
                        try
                            {
                                // Issue a MySQL EXPLAIN command to explain the query and
                                // take the "rows" value returned in the first row as the
                                // estimated cardinality. Note that this will not be correct
                                // for more complex queries.
                                Connection conn = jci.connect();
                                Statement stmt = conn.createStatement();

                                if (stmt.execute("explain " + sqi.formatStatementText(info, 0)))
                                    {
                                        ResultSet rs = stmt.getResultSet();

                                        rs.next();
                                        numEstimatedRows = rs.getLong("rows");
                                        // we only set the estimate in sqi if we got
                                        // a real estimate from MySQL, and we use this
                                        // estimate also to parallelize the query with
                                        // the LIMIT clause
                                        sqi.setEstimatedNumRows(numEstimatedRows);
                                        rs.close();
                                    }

                                jci.disconnect();
                            }
                        catch (SQLException e)
                            {
                                throw new UDRException(
                                    38380,
                                    "SQL Exception when explaining SQL statement for cardinality estimate: %s",
                                    e.getMessage());
                            }
                        break;

                    case ORACLE:
                        if (sqi.getPlanTableName() != null && sqi.getPlanTableName().length() > 0) {
                            try
                                {
                                    // Issue an Oracle EXPLAIN PLAN command to explain the query and
                                    // take the "cardinality" value returned in the first row as the
                                    // estimated cardinality. Note that this requires a table on the
                                    // Oracle database to hold the output of EXPLAIN PLAN and that
                                    // we need to have write access to that table.
                                    Connection conn = jci.connect();
                                    Statement stmt = conn.createStatement();
                                    boolean hasResultSet;

                                    String planExist = "select table_name from all_tables where table_name = '" +
                                                       sqi.getPlanTableName().toUpperCase()+"'";

                                    ResultSet existRS = stmt.executeQuery(planExist);
                                    if (false == existRS.next()) {
                                        String planDDL = "CREATE TABLE "+
                                                  sqi.getPlanTableName()+
                                                  "    ("+
                                                  "        STATEMENT_ID VARCHAR2(30),"+
                                                  "        PLAN_ID NUMBER,"+
                                                  "        TIMESTAMP DATE,"+
                                                  "        REMARKS VARCHAR2(4000),"+
                                                  "        OPERATION VARCHAR2(30),"+
                                                  "        OPTIONS VARCHAR2(255),"+
                                                  "        OBJECT_NODE VARCHAR2(128),"+
                                                  "        OBJECT_OWNER VARCHAR2(30),"+
                                                  "        OBJECT_NAME VARCHAR2(30),"+
                                                  "        OBJECT_ALIAS VARCHAR2(65),"+
                                                  "        OBJECT_INSTANCE INTEGER,"+
                                                  "        OBJECT_TYPE VARCHAR2(30),"+
                                                  "        OPTIMIZER VARCHAR2(255),"+
                                                  "        SEARCH_COLUMNS NUMBER,"+
                                                  "        ID INTEGER,"+
                                                  "        PARENT_ID INTEGER,"+
                                                  "        DEPTH INTEGER,"+
                                                  "        POSITION INTEGER,"+
                                                  "        COST INTEGER,"+
                                                  "        CARDINALITY INTEGER,"+
                                                  "        BYTES INTEGER,"+
                                                  "        OTHER_TAG VARCHAR2(255),"+
                                                  "        PARTITION_START VARCHAR2(255),"+
                                                  "        PARTITION_STOP VARCHAR2(255),"+
                                                  "        PARTITION_ID INTEGER,"+
                                                  "        OTHER LONG,"+
                                                  "        DISTRIBUTION VARCHAR2(30),"+
                                                  "        CPU_COST INTEGER,"+
                                                  "        IO_COST INTEGER,"+
                                                  "        TEMP_SPACE INTEGER,"+
                                                  "        ACCESS_PREDICATES VARCHAR2(4000),"+
                                                  "        FILTER_PREDICATES VARCHAR2(4000),"+
                                                  "        PROJECTION VARCHAR2(4000),"+
                                                  "        TIME INTEGER,"+
                                                  "        QBLOCK_NAME VARCHAR2(30),"+
                                                  "        OTHER_XML CLOB"+
                                                  "    )";
                                        stmt.execute (planDDL);
                                    }
                                    existRS.close();

                                    // make a reasonably unique statement id in 30 chars or less
                                    UUID stmtIdUUID = UUID.randomUUID();
                                    String stmtId =
                                        "s" + stmtIdUUID.hashCode() + stmtIdUUID.getLeastSignificantBits()/1000;

                                    String explainStmt =
                                        "explain plan set statement_id = '" +
                                        stmtId +
                                        "' into " +
                                        sqi.getPlanTableName() +
                                        " for " +
                                        sqi.formatStatementText(info, 0);
                                    stmt.execute(explainStmt);

                                    if (stmt.execute("select cardinality from " +
                                                     sqi.getPlanTableName() +
                                                     " where statement_id = '" +
                                                     stmtId +
                                                     "' and id = 0"))
                                        {
                                            ResultSet rs = stmt.getResultSet();

                                            rs.next();
                                            numEstimatedRows = rs.getLong("cardinality");
                                            if (-1 == numEstimatedRows)
                                                numEstimatedRows = 100;
                                            // we only set the estimate in sqi if we got
                                            // a real estimate from Oracle, and we use this
                                            // estimate also to parallelize the query with
                                            // the ROWNUM predicates
                                            sqi.setEstimatedNumRows(numEstimatedRows);
                                            rs.close();
                                            //System.out.println("describeStatistics numEstimatedRows="+numEstimatedRows);

                                            // auto decide number of patitions. can't do this heres
                                            if (sqi.getManualPartitions() == false && numEstimatedRows > 10000)
                                            {
                                                sqi.setNumberOfPartitions(((int)(numEstimatedRows / 10000)) + 1);
                                                //System.out.println("describeStatistics partitions="+sqi.getNumberOfPartitions());

                                                // max is 16 paritions automaticly
                                                if (sqi.getNumberOfPartitions() > 16)
                                                {
                                                    sqi.setNumberOfPartitions(16);
                                                }
                                            }
                                        }

                                    // clean up
                                    stmt.execute("delete from " +
                                                 sqi.getPlanTableName() +
                                                 " where statement_id = '" +
                                                 stmtId +
                                                 "'");

                                    jci.disconnect();
                                }
                            catch (SQLException e)
                                {
                                    throw new UDRException(
                                        38380,
                                        "SQL Exception when explaining SQL statement for cardinality estimate: %s",
                                        e.getMessage());
                                }
                        }
                        break;

                    default:
                        // don't provide a compiler estimate from the
                        // remote data source
                        break;
                    }
            }
    }

    // determine output columns dynamically at compile time
    @Override
    public void describeParamsAndColumns(UDRInvocationInfo info)
        throws UDRException
    {
        //System.out.println("describeParamsAndColumns : "+System.currentTimeMillis());
        // create an object with common info for this
        // UDF invocation that we will carry through the
        // compilation phases
        info.setUDRWriterCompileTimeData(new JdbcUDRCompileTimeData());

        // retrieve the compile time data, we will do this for
        // every compile phase
        JdbcConnectionInfo jci = getConnectionInfo(info);
        SQLStatementInfo   sqi = getSQLStatementInfo(info);

        // process input parameters
        handleInputParams(info, jci, sqi);
    }

    @Override
    public void describeDataflowAndPredicates(UDRInvocationInfo info)
        throws UDRException
    {
        JdbcConnectionInfo jci = getConnectionInfo(info);
        SQLStatementInfo   sqi = getSQLStatementInfo(info);

        switch (sqi.getStmtType())
            {
            case SOURCE_TABLE_MYSQL:
            case SOURCE_TABLE_ORACLE:
                {
                    // replace the "select * from <table>" statement with one
                    // that lists the needed columns and that may also have
                    // predicates
                    String newSQLStmt = sqi.formatStatementText(info, 0);

                    try
                        {
                            jci.connect();

                            PreparedStatement preparedStmt =
                                jci.getConnection().prepareStatement(newSQLStmt);

                            //jci.disconnect();
                            sqi.addResultProducingStatement(preparedStmt, 0);
                        }
                    catch (SQLException e)
                        {
                            throw new UDRException(
                                38380,
                                "SQL Exception when preparing SQL statement after processing projections and selections: %s. Exception text: %s",
                                newSQLStmt, e.getMessage());
                        }
                } // SOURCE_TABLE_MYSQL and Oracle

            default:
                // no action for other statement types - no
                // elimination of output columns or predicate pushdown
                break;
            } // switch
    }

    @Override
    public void describeConstraints(UDRInvocationInfo info)
        throws UDRException
    {
        JdbcConnectionInfo jci = getConnectionInfo(info);
        SQLStatementInfo   sqi = getSQLStatementInfo(info);

        switch (sqi.getStmtType())
            {
            case SOURCE_TABLE_MYSQL:
                // TBD: Find MySQL constraints
                break;
            case SOURCE_TABLE_ORACLE:
                try
                    {
                        // issue an Oracle metadata query to list all the
                        // primary key constraints, unique indexes and/or
                        // uniqueness constraints
                        Connection conn = jci.connect();
                        Statement stmt = conn.createStatement();
                        boolean hasResultSet;
                        String tableName = sqi.getTableName();

                        // upshift non-delimited Oracle names, this is
                        // how they are stored in the metadata
                        if (tableName.indexOf('"') < 0)
                            tableName = tableName.toUpperCase();
                        tableName.replaceAll("'", "''");

                        String constraintQuery =
                            "select x.index_name, c.column_name, c.column_position from USER_INDEXES x join USER_IND_COLUMNS c on x.index_name = c.index_name and x.table_name = c.table_name left join USER_CONSTRAINTS uc on x.index_name = uc.index_name and x.table_name = uc.table_name where x.status = 'VALID' and (x.uniqueness = 'UNIQUE' or uc.constraint_type = 'U' and uc.status = 'ENABLED' and uc.validated = 'VALIDATED') and x.table_name= '" +
                            tableName +
                            "' order by x.index_name, c.column_position";

                        if (stmt.execute(constraintQuery))
                            {
                                ResultSet rs = stmt.getResultSet();
                                String currIndexName = null;
                                UniqueConstraintInfo currConstraint = null;
                                boolean currConstraintIsValid = false;

                                while (rs.next())
                                    {
                                        String indexName = rs.getString(1);
                                        String colName = rs.getString(2);
                                        int colPos = rs.getInt(3);
                                        int udfOutputColPos = -1;

                                        if (indexName != null &&
                                            indexName != currIndexName)
                                            {
                                                // start a new unique constraint
                                                // for this index, add the previous
                                                // one if it exists
                                                if (currConstraint != null && currConstraintIsValid)
                                                    info.out().addUniquenessConstraint(currConstraint);
                                                currConstraint = new UniqueConstraintInfo();
                                                currIndexName = indexName;
                                                currConstraintIsValid = true;
                                            }

                                        try
                                            {
                                                udfOutputColPos = info.out().getColNum(colName);
                                                currConstraint.addColumn(udfOutputColPos);
                                            }
                                        catch (UDRException e)
                                            {
                                                // There could be various reasons why this
                                                // could fail, the main one being that the
                                                // unique column has been eliminated. We
                                                // could also have esoteric cases of
                                                // delimited identifiers.
                                                // Just invalidate this constraint and
                                                // move on.
                                                currConstraintIsValid = false;
                                            }
                                    } // loop over result set
                                rs.close();

                                // add the last (maybe only) constraint
                                if (currConstraint != null && currConstraintIsValid)
                                    info.out().addUniquenessConstraint(currConstraint);
                            } // metadata query had result set

                        //jci.disconnect();
                    }
                catch (SQLException e)
                    {
                        throw new UDRException(
                            38380,
                            "SQL Exception when determining constraints: %s",
                            e.getMessage());
                    }
                break;

            default:
                // no action for other statement types - no
                // elimination of output columns or predicate pushdown
                break;
            } // switch
    }

    @Override
    public void describeStatistics(UDRInvocationInfo info)
        throws UDRException
    {
        SQLStatementInfo   sqi = getSQLStatementInfo(info);

        info.out().setEstimatedNumRows(sqi.getEstimatedNumRows());
    }

    @Override
    public void describeDesiredDegreeOfParallelism(UDRInvocationInfo info,
                                                   UDRPlanInfo plan)
         throws UDRException
    {
        SQLStatementInfo   sqi = getSQLStatementInfo(info);

        //System.out.println("describeDesiredDegreeOfParallelism partitions="+sqi.getNumberOfPartitions());
        plan.setDesiredDegreeOfParallelism(sqi.getNumberOfPartitions());
    }

    @Override
    public void completeDescription(UDRInvocationInfo info,
                                    UDRPlanInfo plan)
        throws UDRException
    {
        SQLStatementInfo   sqi = getSQLStatementInfo(info);

        // right now we only pass data from compile time to runtime
        // for the flavors that access a single table
        if (sqi.getIsSingleTable())
            {
                JdbcUDRPlanData dataForRuntime = new JdbcUDRPlanData();

                dataForRuntime.dop_ = sqi.getNumberOfPartitions();
                dataForRuntime.estNumRows_ = sqi.getEstimatedNumRows();
                dataForRuntime.formattedStatement_ = sqi.formatStatementText(info, 0);

                dataForRuntime.addToPlan(plan);
            }
    }

    // override the runtime method
    @Override
    public void processData(UDRInvocationInfo info,
                            UDRPlanInfo plan)
        throws UDRException
    {
        // jci and sqi are rebuilt from the input parameters
        JdbcConnectionInfo jci = new JdbcConnectionInfo();
        SQLStatementInfo   sqi = new SQLStatementInfo();

        // planData is the data we get from the compiler phase
        JdbcUDRPlanData    planData = new JdbcUDRPlanData(plan);

        // process input parameters (again, now at runtime)
        handleInputParams(info, jci, sqi);

        if (planData.estNumRows_ > 0)
            // transfer the row count estimate from the compiler phase
            // to sqi, as we will not repeat this estimate at runtime
            sqi.setEstimatedNumRows(planData.estNumRows_);

        int numCols = info.out().getNumColumns();
        int numSQLStatements = sqi.getNumStatements();
        int numSQLResultSets = 0;
        int numPartitions = sqi.getNumberOfPartitions();
        int numParallelInstances = info.getNumParallelInstances();
        int myInstanceNum = info.getMyInstanceNum();
        String stmtText = "connect";

        try {
            Connection conn = jci.connect();
            Statement stmt = conn.createStatement();

            for (int s=0; s<numSQLStatements; s++)
            {
                boolean stmtHasResultSet = false;

                // round-robin the partitions defined by the caller
                // over the parallel instances, each parallel instance
                // could get 0, 1 or more than 1 partition(s)
                for (int partNum = myInstanceNum;
                     partNum < numPartitions;
                     partNum += numParallelInstances)
                    {
                        stmtText = sqi.formatStatementText(info, s, partNum);

                        // debug : write query to file
                        //try
                        //{
                        //  FileOutputStream out = new FileOutputStream(new File("/home/esgyndb/workspace/code/test_jdbc_udr/sql.txt"),true);
                        //  out.write((stmtText+"\n").getBytes());
                        //  out.flush();
                        //  out.close();
                        //}
                        //catch (Exception e) {
                        //
                        //}

                        boolean hasResultSet = stmt.execute(stmtText);

                        if (hasResultSet && partNum == myInstanceNum)
                            {
                                numSQLResultSets++;
                                stmtHasResultSet = true;
                            }

                        if (sqi.getDebugMode())
                            {
                                // In debug mode, generate a row with diagnostic info, not
                                // the actual rows. Note that for statement types that
                                // determine the columns dynamically, the first column
                                // selected must be a string column that is long enough.
                                String diags1 = String.format(
                                  "Instance num: %d, statement num: %d, partition num %d, has resultset: %b, statement: %s",
                                  myInstanceNum,
                                  s,
                                  partNum,
                                  hasResultSet,
                                  stmtText);
                                info.out().setString(0, diags1);

                                emitRow(info);
                                break;
                            }

                        if (hasResultSet != stmtHasResultSet)
                            throw new UDRException(
                              38700,
                              "Partition %d of statement %s is not returning a result set while the previous partition %d did, or vice versa (UDF %s)",
                              partNum,
                              stmtText,
                              partNum-numParallelInstances,
                              info.getUDRName());

                        if (hasResultSet)
                            {
                                ResultSet rs = stmt.getResultSet();

                                if (numSQLResultSets > 1)
                                    throw new UDRException(
                                      38700,
                                      "More than one result set returned by UDF %s",
                                      info.getUDRName());

                                if (rs.getMetaData().getColumnCount() != numCols)
                                    throw new UDRException(
                                      38702,
                                      "Number of columns returned by UDF %s (%d) differs from the number determined at compile time (%d)",
                                      info.getUDRName(),
                                      rs.getMetaData().getColumnCount(),
                                      numCols);

                                while (rs.next())
                                    {
                                        for (int c=0; c<numCols; c++)
                                            {
                                                TypeInfo typ = info.out().getColumn(c).getType();

                                                switch (typ.getSQLTypeSubClass())
                                                    {
                                                    case FIXED_CHAR_TYPE:
                                                    case VAR_CHAR_TYPE:
                                                        {
                                                            String str = rs.getString(c+1);
                                                            // workaround for TRAFODION-2546
                                                            if (str != null)
                                                                info.out().setString(c,str);
                                                            else
                                                                info.out().setNull(c);
                                                        }
                                                        break;

                                                    case EXACT_NUMERIC_TYPE:
                                                        switch (typ.getSQLType())
                                                            {
                                                                case NUMERIC:
                                                                case DECIMAL_LSE:
                                                                case NUMERIC_UNSIGNED:
                                                                case DECIMAL_UNSIGNED:
                                                                    /*
                                                                     * Fix ISSUE08: need to calculate value for Oracle and Mysql Decimal/Number types,
                                                                     * or decimal value 1234.5600(8,4) will be selected as 12.3456(8,4) from TMUDF
                                                                     */
                                                                    BigDecimal dec = rs.getBigDecimal(c+1);
                                                                    //System.out.println("BigDecimal = " + dec);

                                                                    if (null != dec)
                                                                    {
                                                                       long value = dec.unscaledValue().longValue();
                                                                       value *= Math.pow(10, typ.getScale() - dec.scale());
                                                                       info.out().setLong(c, value);

                                                                    }
                                                                    else
                                                                    {
                                                                        info.out().setNull(c);
                                                                    }
                                                                    break;

                                                                default:
                                                                    info.out().setLong(c, rs.getLong(c+1));
                                                                    break;
                                                            }

                                                        break;

                                                    case APPROXIMATE_NUMERIC_TYPE:
                                                        info.out().setDouble(c, rs.getDouble(c+1));
                                                        break;

                                                    case DATE_TYPE:
                                                        {
                                                            Date dt = rs.getDate(c+1);
                                                            // workaround for TRAFODION-2546
                                                            if (dt != null)
                                                                info.out().setTime(c, dt);
                                                            else
                                                                info.out().setNull(c);
                                                        }
                                                        break;

                                                    case TIME_TYPE:
                                                        {
                                                            Time tim = rs.getTime(c+1);
                                                            // workaround for TRAFODION-2546
                                                            if (tim != null)
                                                                info.out().setTime(c, tim);
                                                            else
                                                                info.out().setNull(c);
                                                        }
                                                        break;

                                                    case TIMESTAMP_TYPE:
                                                        info.out().setTime(c, rs.getTimestamp(c+1));
                                                        break;

                                                    case LOB_SUB_CLASS:
                                                        throw new UDRException(38710, "LOB parameters not yet supported");

                                                    default:
                                                        throw new UDRException(38720, "Unexpected data type encountered");

                                                    } // switch

                                                if (rs.wasNull())
                                                    info.out().setNull(c);
                                            } // loop over columns

                                        // produce a result row
                                        emitRow(info);

                                    } // loop over result rows
                                rs.close();
                            } // statement produces a result set
                    } // loop over partitions
            } // loop over statements
            jci.disconnect();
        } catch (SQLException e) {
            throw new UDRException(
                    38730,
                    "JDBC error for statement %s at runtime: %s",
                    stmtText,
                    e.getMessage());
        }

    }

};
