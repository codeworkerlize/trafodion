/**********************************************************************
* @@@ START COPYRIGHT @@@
*
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
*
* @@@ END COPYRIGHT @@@
**********************************************************************/
package org.trafodion.dcs.util;

import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.trafodion.dcs.Constants;

public final class JdbcT2Util
{
	private static final Logger LOG = LoggerFactory.getLogger(JdbcT2Util.class);
    private Configuration conf;
    private int mapInitialSize;
    Map<String, ConnectionContext> m;
 
	static
	{
		try {
			Class.forName(Constants.T2_DRIVER_CLASS_NAME);
		} catch (ClassNotFoundException e) {
			LOG.error(e.getMessage(), e);
		}
	}
	
	public void init(Configuration conf) {
		this.conf = conf;
		mapInitialSize = conf.getInt("dcs.info.threads.max", 100);
		m = new HashMap<String, ConnectionContext>(mapInitialSize);
		System.out.println(Constants.DCS_CONF_DIR + "=" + GetJavaProperty.getProperty(Constants.DCS_CONF_DIR));
		System.out.println(Constants.DCS_ROOT_LOGGER + "=" + GetJavaProperty.getProperty(Constants.DCS_ROOT_LOGGER));
		System.out.println(Constants.DCS_LOG_DIR + "=" + GetJavaProperty.getProperty(Constants.DCS_LOG_DIR));
		System.out.println(Constants.DCS_LOG_FILE + "=" + GetJavaProperty.getProperty(Constants.DCS_LOG_FILE));
		
		GetJavaProperty.setProperty("hbaseclient.log4j.properties",GetJavaProperty.getProperty(Constants.DCS_CONF_DIR) + "/log4j.properties");
		GetJavaProperty.setProperty(Constants.DCS_ROOT_LOGGER,GetJavaProperty.getProperty(Constants.DCS_ROOT_LOGGER));
		GetJavaProperty.setProperty(Constants.DCS_LOG_DIR,GetJavaProperty.getProperty(Constants.DCS_LOG_DIR));
		GetJavaProperty.setProperty(Constants.DCS_LOG_FILE,GetJavaProperty.getProperty(Constants.DCS_LOG_FILE));
	}
	
	public JdbcT2Util() {
		Configuration conf = DcsConfiguration.create();
		init(conf);
	}
	
	public JdbcT2Util(Configuration conf) {
		init(conf);
	}

    class ConnectionContext {
        java.sql.Connection conn;
        java.sql.Statement stmt;
        java.sql.ResultSet rs; 
        boolean open = false;
        boolean error = false;

		void open() {
			LOG.debug("Begin ConnectionContext.open()");
			
			if(open == false) {
				try	{
					LOG.debug("DriverManager.getConnection({}).", Constants.T2_DRIVER_URL);
					conn = DriverManager.getConnection(Constants.T2_DRIVER_URL);
					LOG.debug("conn.createStatement()");
					stmt = conn.createStatement();
					LOG.debug("stmt.execute({})", Constants.CQD_ESTIMATE_HBASE_ROW_COUNT_OFF);
					stmt.execute(Constants.CQD_ESTIMATE_HBASE_ROW_COUNT_OFF);
					open = true;
				} catch (SQLException e) {
					SQLException nextException;
					nextException = e;
					StringBuilder sb = new StringBuilder();
					do {
						sb.append(nextException.getMessage());
						sb.append("\nSQLState   " + nextException.getSQLState());
						sb.append("\nError Code " + nextException.getErrorCode());
					} while ((nextException = nextException.getNextException()) != null);
					LOG.error("SQLException [" + sb.toString() + "]", e);
					error = true;
				} catch (Exception e) {
					StringBuilder sb = new StringBuilder();
					sb.append(e.getMessage());
					LOG.error("Exception [" + sb.toString() + "]", e);
					error = true;
				}
			} else {
				LOG.debug("connection is open");
			}
			
			LOG.debug("End ConnectionContext.open()");
		}

		void execute(String text){
			LOG.debug("Begin ConnectionContext.execute()");
				
			try	{
				open();
				LOG.debug("stmt.executeQuery({})",text);
				rs = stmt.executeQuery(text);
			} catch (SQLException e) {
				SQLException nextException;
				nextException = e;
				StringBuilder sb = new StringBuilder();
				do {
					sb.append(nextException.getMessage());
					sb.append("\nSQLState   " + nextException.getSQLState());
					sb.append("\nError Code " + nextException.getErrorCode());
				} while ((nextException = nextException.getNextException()) != null);
				LOG.error("SQLException [" + sb.toString() + "]", e);
				error = true;
			} catch (Exception e) {
				LOG.error(e.getMessage(), e);
				error = true;
			}
			
			if(LOG.isDebugEnabled())
				LOG.debug("End ConnectionContext.execute()");
		}
        java.sql.ResultSet getResultSet(){
            return rs;
        }
        
        boolean isOpen(){
            return open;
        }
        
        boolean isError(){
            return error;
        }
    }
    
    public synchronized JSONArray exec(String command){
        LOG.debug("Begin exec()");

		JSONArray js = null;
		String threadId = Thread.currentThread().getName();
		LOG.debug("threadId <{}>", threadId);
		
		try	{
			ConnectionContext connectionContext = m.get(threadId);
			if(connectionContext == null){
				connectionContext = new ConnectionContext();
			    LOG.debug("new ConnectionContext({})", threadId);
				m.put(threadId,connectionContext);
			    LOG.debug("m.put({},{})",threadId,connectionContext);
			}
			
			LOG.debug("connectionContext <{}>", connectionContext);
			js = new JSONArray();
			connectionContext.execute(command);
			if(! connectionContext.isError()) {
				js = convertResultSetToJSON(connectionContext.getResultSet());
			} else {
				LOG.debug("Error - m.remove({})", threadId);
				m.remove(threadId);
			}
		} catch (Exception e) {
			LOG.error(e.getMessage(), e);
		}
		
		LOG.debug("End exec()");

        return js;
    }
    
    private synchronized JSONArray convertResultSetToJSON(java.sql.ResultSet rs) throws Exception {
        LOG.debug("Begin convertResultSetToJSON");
        
        JSONArray json = new JSONArray();

        try { 

            java.sql.ResultSetMetaData rsmd = rs.getMetaData(); 

            while(rs.next()){ 
                int numColumns = rsmd.getColumnCount(); 
                JSONObject obj = new JSONObject(); 

                for (int i=1; i<numColumns+1; i++) { 

                    String column_name = rsmd.getColumnName(i); 

                    if(rsmd.getColumnType(i)==java.sql.Types.ARRAY){ 
                        obj.put(column_name, rs.getArray(column_name)); 
                    } 
                    else if(rsmd.getColumnType(i)==java.sql.Types.BIGINT){ 
                        obj.put(column_name, rs.getLong(column_name)); 
                    } 
                    else if(rsmd.getColumnType(i)==java.sql.Types.BOOLEAN){ 
                        obj.put(column_name, rs.getBoolean(column_name)); 
                    } 
                    else if(rsmd.getColumnType(i)==java.sql.Types.BLOB){ 
                        obj.put(column_name, rs.getBlob(column_name)); 
                    } 
                    else if(rsmd.getColumnType(i)==java.sql.Types.DOUBLE){ 
                        obj.put(column_name, rs.getDouble(column_name));  
                    } 
                    else if(rsmd.getColumnType(i)==java.sql.Types.FLOAT){ 
                        obj.put(column_name, rs.getFloat(column_name)); 
                    } 
                    else if(rsmd.getColumnType(i)==java.sql.Types.INTEGER){ 
                        obj.put(column_name, rs.getInt(column_name)); 
                    } 
                    else if(rsmd.getColumnType(i)==java.sql.Types.NVARCHAR){ 
                        obj.put(column_name, rs.getNString(column_name)); 
                    } 
                    else if(rsmd.getColumnType(i)==java.sql.Types.CHAR || 
                            rsmd.getColumnType(i)==java.sql.Types.VARCHAR){ 
                        //prevent obj.put from removing null key value from JSONObject
                        String s = rs.getString(column_name);
                        if(s == null)
                            obj.put(column_name, new String("")); 
                        else
                            obj.put(column_name, rs.getString(column_name)); 
                    }                     
                    else if(rsmd.getColumnType(i)==java.sql.Types.TINYINT){ 
                        obj.put(column_name, rs.getInt(column_name)); 
                    } 
                    else if(rsmd.getColumnType(i)==java.sql.Types.SMALLINT){ 
                        obj.put(column_name, rs.getInt(column_name)); 
                    } 
                    else if(rsmd.getColumnType(i)==java.sql.Types.DATE){ 
                        obj.put(column_name, rs.getDate(column_name)); 
                    } 
                    else if(rsmd.getColumnType(i)==java.sql.Types.TIMESTAMP){ 
                        obj.put(column_name, rs.getTimestamp(column_name));    
                    } 
                    else{ 
                        obj.put(column_name, rs.getObject(column_name)); 
                    }  

                }//end foreach 
                json.put(obj); 

            }//end while 

        } catch (SQLException e) { 
            LOG.error(e.getMessage(), e);
            throw e;
        } catch (Exception e) { 
            LOG.error(e.getMessage(), e);
            throw e;
        }
        
        LOG.debug("End convertResultSetToJSON");

		return json; 
	}
	
	public static void main(String args[])
	{
		Options opt = new Options();
		CommandLine commandLine;
		String command = null;
		
		StringBuilder sb = new StringBuilder();
		
		try {
			commandLine = new GnuParser().parse(opt, args);
			command = commandLine.getArgList().get(0).toString();
		} catch (Exception e) {
			sb.append("Command [" + command + "], ");
			sb.append("Exception: " + e.getMessage());
			LOG.error(sb.toString(), e);
			System.exit(1);
		}

		try	{
			Log4jUtils log4jutils = new Log4jUtils();
			JdbcT2Util jdbcT2Util = new JdbcT2Util();
			if(LOG.isDebugEnabled())
				log4jutils.dumpLog4j(JdbcT2Util.class.getName());
			JSONArray js = jdbcT2Util.exec(command);
			if(LOG.isDebugEnabled())
				log4jutils.dumpLog4j(JdbcT2Util.class.getName());
			LOG.debug("JSONArray <{}>", js.toString());
		} catch (Exception e) {
			sb.append(e.getMessage());
            LOG.error("Exception <{}>", sb.toString(), e);
			System.exit(1);
		}
	}
	
}

