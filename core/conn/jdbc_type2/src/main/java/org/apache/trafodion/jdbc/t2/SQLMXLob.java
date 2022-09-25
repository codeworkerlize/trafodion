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

/* -*-java-*-
 * Filename    : SQLMXLob.java
 * Description : SQLMXClob and SQLMXBlob extends this class. Some of the 
 *     common methods are implemented here
 */

package org.apache.trafodion.jdbc.t2;

import java.sql.*;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.IOException;
import java.util.Date;
import java.util.Arrays;
import java.io.PrintWriter;

public abstract class SQLMXLob
{
	// There is some discrepancy if LOB_V2_EXT_HANDLE_LEN should be 96 or 100
	// In java layer, this constant is being used to figure out
	// where inline data starts and hence changed to 96
	static final int LOB_V2_EXT_HANDLE_LEN = 96;
        long inLength() throws SQLException 
        {
		if (data_ != null && length_ == 0)
			return data_.length;
		else
			return length_;
        }

	public long length() throws SQLException
	{
		long length = inLength();
		if(length != 0 ) {
			return length;
		}else{
			return getLobLength(conn_.server_, conn_.getDialogueId(), stmt_.getStmtId(),
					lobVersion_, getLobTableUid(), lobLocator_);
		}
	}

	public void truncate(long len) throws SQLException
	{
		if (len > 0)  
			Messages.throwUnsupportedFeatureException(conn_.locale_, "Blob/Clob.truncate() with len > 0"); 
		truncate(conn_.server_, conn_.getDialogueId(), stmt_.getStmtId(), 
				lobVersion_, getLobTableUid(), lobLocator_, len);
	}

	InputStream getInputStream(long pos, long length) throws SQLException
	{
		if (JdbcDebugCfg.entryActive) debug[methodId_getInputStream].methodEntry();
		try
		{
			if (inputStream_ != null)
			{
				try
				{
					inputStream_.close();
				}
				catch (IOException e)
				{
				}
				finally
				{
					inputStream_ = null;
				}
			}
			inputStream_ = new SQLMXLobInputStream(stmt_, conn_, this, pos, length);
			return inputStream_;

		}
		finally
		{
			if (JdbcDebugCfg.entryActive) debug[methodId_getInputStream].methodExit();
		}
	}

	SQLMXLobOutputStream setOutputStream(long startingPos) throws SQLException
	{
		outputStream_ = new SQLMXLobOutputStream(stmt_, conn_, startingPos, this);
		return outputStream_;
	}

	void resetOutputStream()
	{
		outputStream_ = null;
	}


	void close() throws SQLException
	{
		try {
			if (inputStream_ != null)
				inputStream_.close();
			if (outputStream_ != null)
				outputStream_.close();
			inputStream_ = null;
			outputStream_ = null;
			isCurrent_ = false;
		} catch (IOException ioe) {
			throw new SQLException(ioe);
		}
	}


	static String convSQLExceptionToIO(SQLException e)
	{
		if (JdbcDebugCfg.entryActive) debug[methodId_convSQLExceptionToIO].methodEntry();
		try
		{
			SQLException e1;
			e1 = e;
			StringBuffer s = new StringBuffer(1000);
			do
			{
				s.append("SQLState :");
				s.append(e1.getSQLState());
				s.append(" ErrorCode :");
				s.append(e1.getErrorCode());
				s.append(" Message:");
				s.append(e1.getMessage());
			}
			while ((e1 = e1.getNextException()) != null);
			return s.toString();
		}
		finally
		{
			if (JdbcDebugCfg.entryActive) debug[methodId_convSQLExceptionToIO].methodExit();
		}
	}

        void checkIfCurrent() throws SQLException
	{
		if (JdbcDebugCfg.entryActive) debug[methodId_checkIfCurrent].methodEntry();
		try
		{
			if (! isCurrent_)
			{
				Object[] messageArguments = new Object[1];
				messageArguments[0] = this;
				throw Messages.createSQLException(conn_.locale_, "lob_not_current",
					messageArguments);
			}
		}
		finally
		{
			if (JdbcDebugCfg.entryActive) debug[methodId_checkIfCurrent].methodExit();
		}
	}

	String getLobLocator()
	{
		return lobLocator_;
	}

	long getLobTableUid() throws SQLException
	{
		if (lobTableUid_ == -1) {
			String tableUidStr = lobLocator_.substring(16, 36); 
			try {	
				lobTableUid_ = Long.parseLong(tableUidStr);	
			} catch (NumberFormatException ne) {
				Object[] messageArguments = new Object[1];
				messageArguments[0] = tableUidStr;
				throw Messages.createSQLException(conn_.locale_, "invalid_lob_table_uid:", messageArguments);
			}
		}
		return lobTableUid_;
	}
	
	void setLobLocator(String inlineData) 
	{
		if (inlineData == null)
			return;
		if (lobVersion_ == 1)
			lobLocator_ = inlineData;
		else if (inlineData.length() <= LOB_V2_EXT_HANDLE_LEN)
			lobLocator_ = inlineData;
		else {
			lobLocator_ = inlineData.substring(0, LOB_V2_EXT_HANDLE_LEN);
			inlineDataString_ = inlineData.substring(LOB_V2_EXT_HANDLE_LEN);
			length_ = Long.parseLong(lobLocator_.substring(36, 46));
			if (this instanceof SQLMXClob) {
				length_ = inlineDataString_.length();
			}
		}			
	}

	void setLobLocator(byte[] inlineData) 
	{
		if (inlineData == null)
			return;
		if (lobVersion_ == 1)
			lobLocator_ = new String(inlineData);
		else if (inlineData.length <= LOB_V2_EXT_HANDLE_LEN)
			lobLocator_ = new String(inlineData);
		else {
			lobLocator_ = new String(inlineData, 0, LOB_V2_EXT_HANDLE_LEN);
			inlineDataBytes_ = Arrays.copyOfRange(inlineData, LOB_V2_EXT_HANDLE_LEN, inlineData.length);
			length_ = Long.parseLong(lobLocator_.substring(36, 46));
			if (this instanceof SQLMXClob) {
				length_ = inlineData.length - LOB_V2_EXT_HANDLE_LEN;
			}
		}			
	}

	SQLMXLob(SQLMXConnection conn, boolean isBlob)
	{
		conn_ = conn;
		isBlob_ = isBlob;
		lobWithExternalData_ = true;
	}

	SQLMXLob(SQLMXStatement stmt, SQLMXDesc lobDesc,  String inlineData, boolean isBlob) throws SQLException
	{
		stmt_ = stmt;
		conn_ = stmt.getConnectionImpl();
		lobVersion_ = lobDesc.getLobVersion();
		isBlob_ = isBlob;
		setLobLocator(inlineData);
		chunkSize_ = lobDesc.getLobChunkSize();
		isCurrent_ = true;
		lobTableUid_ = -1;
		lobWithExternalData_ = false;
        }

	SQLMXLob(SQLMXStatement stmt, SQLMXDesc lobDesc, byte[] inlineData, boolean isBlob, byte[] data) throws SQLException
	{
		stmt_ = stmt;
		conn_ = stmt.getConnectionImpl();
		lobVersion_ = lobDesc.getLobVersion();
		isBlob_ = isBlob;
		setLobLocator(inlineData);
		chunkSize_ = lobDesc.getLobChunkSize();
		isCurrent_ = true;
		lobTableUid_ = -1;
		data_ = data;
		lobWithExternalData_ = false;
	}

	SQLMXLob(SQLMXStatement stmt, SQLMXDesc lobDesc, String inlineData, boolean isBlob, byte[] data) throws SQLException
	{
		stmt_ = stmt;
		conn_ = stmt.getConnectionImpl();
		lobVersion_ = lobDesc.getLobVersion();
		isBlob_ = isBlob;
		setLobLocator(inlineData);
		chunkSize_ = lobDesc.getLobChunkSize();
		isCurrent_ = true;
		lobTableUid_ = -1;
		data_ = data;
		lobWithExternalData_ = false;
	}

	SQLMXLob(SQLMXStatement stmt, SQLMXDesc lobDesc, String inlineData, InputStream x, long length, boolean isBlob) throws SQLException
	{
		this(stmt, lobDesc, inlineData, isBlob);
		is_ = x;
		length_ = length;
	}

	public void setTraceId(String traceId_) {
		this.traceId_ = traceId_;
	}

	public String getTraceId() {
		traceWriter_ = SQLMXDataSource.traceWriter_;

		// Build up template portion of jdbcTrace output. Pre-appended to jdbcTrace entries.
		// jdbcTrace:[XXXX]:[Thread[X,X,X]]:[XXXXXXXX]:ClassName.
		if (traceWriter_ != null)
		{
			traceFlag_ = T2Driver.traceFlag_;
			String className = getClass().getName();
			setTraceId(T2Driver.traceText + T2Driver.dateFormat.format(new Date())
				+ "]:[" + Thread.currentThread() + "]:[" + hashCode() +  "]:"
				+ className.substring(T2Driver.REMOVE_PKG_NAME,className.length())
				+ ".");
		}
		return traceId_;
	}

	native long getLobLength(String server, long dialogueId, long stmtId, short lobVersion, long lobTableUid, String lobLocator);
	native void truncate(String server, long dialogueId, long stmtId, short lobVersion, long lobTableUid, String lobLocator, long len);

	// fields
	private String			traceId_;
	static PrintWriter		traceWriter_;
	static int			traceFlag_;
	SQLMXStatement			stmt_;
	SQLMXConnection			conn_;
	String				lobLocator_;
	SQLMXLobInputStream		inputStream_;
	SQLMXLobOutputStream		outputStream_;
	boolean				isCurrent_;
	InputStream			is_;
	byte[]				data_;
	long				length_;
	int				offset_;
	int				chunkSize_;
	boolean				isBlob_;
	short				lobVersion_;
	String				inlineDataString_;
	byte[]				inlineDataBytes_;
	private long			lobTableUid_;
	boolean				lobWithExternalData_;

	private static int methodId_length				=  0;
	private static int methodId_truncate				=  1;
	private static int methodId_getInputStream			=  2;
	private static int methodId_setOutputStream			=  3;
	private static int methodId_close				=  4;
	private static int methodId_convSQLExceptionToIO		=  5;
	private static int methodId_checkIfCurrent			=  6;
	private static int methodId_SQLMXLob_LLJL			=  7;
	private static int methodId_SQLMXLob_LLJLIL			=  8;
	private static int totalMethodIds				=  9;
	private static JdbcDebug[] debug;

	static
	{
		String className = "SQLMXLob";
		if (JdbcDebugCfg.entryActive)
		{
			debug = new JdbcDebug[totalMethodIds];
			debug[methodId_length] = new JdbcDebug(className,"length");
			debug[methodId_truncate] = new JdbcDebug(className,"truncate");
			debug[methodId_getInputStream] = new JdbcDebug(className,"getInputStream");
			debug[methodId_setOutputStream] = new JdbcDebug(className,"setOutputStream");
			debug[methodId_close] = new JdbcDebug(className,"close");
			debug[methodId_convSQLExceptionToIO] = new JdbcDebug(className,"convSQLExceptionToIO");
			debug[methodId_checkIfCurrent] = new JdbcDebug(className,"checkIfCurrent");
			debug[methodId_SQLMXLob_LLJL] = new JdbcDebug(className,"SQLMXLob[LLJL]");
			debug[methodId_SQLMXLob_LLJLIL] = new JdbcDebug(className,"SQLMXLob[LLJLIL]");
		}
	}
}
