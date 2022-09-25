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
 * Filename		: SQLMXBlob.java
 * Description	: This program implements the java.sql.Blob interface
 *
 */

package org.apache.trafodion.jdbc.t2;

import java.sql.*;
import java.io.InputStream;
import java.io.Reader;
import java.io.OutputStream;
import java.io.Writer;
import java.io.IOException;
import java.util.Date;
import java.io.PrintWriter;
import java.io.ByteArrayInputStream;
import java.util.Arrays;
import java.nio.ByteBuffer;

public class SQLMXBlob extends SQLMXLob implements Blob 
{
	public InputStream getBinaryStream(long pos, long length) throws SQLException
	{
		if (JdbcDebugCfg.entryActive) debug[methodId_getBinaryStream].methodEntry();
		try
		{
			if (lobWithExternalData_) {
				if (data_ != null) {
					is_ = new ByteArrayInputStream(data_, offset_, data_.length);
				} 
				if (is_ != null)
					return is_;
				else
					throw Messages.createSQLException(conn_.locale_, "external_lob_has_no_data", null);
			}
			return getInputStream(pos, length);
		}
		finally
		{
			if (JdbcDebugCfg.entryActive) debug[methodId_getBinaryStream].methodExit();
		}
	}


	public byte[] getBytes(long pos, int length) throws SQLException
	{
	 	long skippedLen;	
		InputStream is = getInputStream(pos, length);
		try {
			byte[] buf = new byte[length];
			int retLen = is.read(buf, 0, length);
			if (retLen < length)
				buf = Arrays.copyOf(buf, retLen);
			return buf;
		} catch (IOException ioe) {
			throw new SQLException(ioe);
		}
	}

	public long position(Blob pattern, long start) throws SQLException
	{
		return position(pattern.getBytes(1, (int)pattern.length()), start);
	}

	public long position(byte[] pattern, long start) throws SQLException
	{
		if (lobWithExternalData_) {
			Object[] messageArguments = new Object[1];
			messageArguments[0] = this;
			throw Messages.createSQLException(conn_.locale_,"invalid_lob_type", messageArguments);
		}
		SQLMXLobInputStream is = (SQLMXLobInputStream)(getBinaryStream(start, Long.MAX_VALUE));
		int readLen;
		int index;
		long retPosition = -1;

		while ((readLen = is.readChunk()) != -1) {
			if ((index = findBytes(is.chunk_, is.currentPos_, pattern)) != -1) {
				retPosition =  is.getBeginLobPos() + index;	 
				break;
			}
		}
		return retPosition;
	}

	public OutputStream setBinaryStream(long pos) throws SQLException
	{
		if (JdbcDebugCfg.entryActive) debug[methodId_setBinaryStream].methodEntry();
		try
		{
			if (pos < 0) {
				Object[] messageArguments = new Object[1];
				messageArguments[0] = "Blob.setBinaryStream(long)";
				throw Messages.createSQLException(conn_.locale_,"invalid_input_value", messageArguments);
			}
			// Check if Autocommit is set, and no external transaction exists
			return setOutputStream(pos);
		}
		finally
		{
			if (JdbcDebugCfg.entryActive) debug[methodId_setBinaryStream].methodExit();
		}
	}

	public int setBytes(long pos, byte[] bytes) throws SQLException
	{
		if (JdbcDebugCfg.entryActive) debug[methodId_setBytes_JB].methodEntry();
		try
		{
			if (bytes == null || pos < 0) {
				Object[] messageArguments = new Object[1];
				messageArguments[0] = "Blob.setBytes(long, byte[])";
				throw Messages.createSQLException(conn_.locale_,"invalid_input_value", messageArguments);
			}
			return setBytes(pos, bytes, 0, bytes.length);
		}
		finally
		{
			if (JdbcDebugCfg.entryActive) debug[methodId_setBytes_JB].methodExit();
		}
	}

	public int setBytes(long pos, byte[] bytes, int offset, int len) throws SQLException
	{
		if (JdbcDebugCfg.entryActive) debug[methodId_setBytes_JBII].methodEntry();
		try
		{
			if (pos < 0 || len < 0 || offset < 0 || bytes == null) 
			{
				Object[] messageArguments = new Object[1];
				messageArguments[0] = "Blob.setBytes(long, byte[], int, int)";
				throw Messages.createSQLException(conn_.locale_,"invalid_input_value", messageArguments);
			}
			data_ = bytes;  	
			length_ = len;
			offset_ = offset;
			return len;
		}
		finally
		{
			if (JdbcDebugCfg.entryActive) debug[methodId_setBytes_JBII].methodExit();
		}
	}

	byte[] getBytes(int inlineLobLen) throws SQLException 
	{
		long llength  = inLength();
		if (llength > Integer.MAX_VALUE) {
			Object[] messageArguments = new Object[1];
			messageArguments[0] = "Blob.getBytes(int)";
			throw Messages.createSQLException(conn_.locale_,"invalid_input_value", messageArguments);
		}
		int length = (int)llength;
		if (length == 0) {
			if (data_ != null && (data_.length - offset_)  > inlineLobLen)	
				return null;
			else
				return null;
		} else if (length_ > inlineLobLen)
			return null;
		if (data_ != null) {
			if (length == 0)
				length = data_.length;
			if (offset_ == 0) {
				if (length_ == 0) 
					return data_;
				else
					return Arrays.copyOf(data_, length);
			}
			else  
				return Arrays.copyOfRange(data_, offset_, offset_+length);
		}
		if (is_ != null) {
			try {
				byte buf[] = new byte[length]; 
				int retLen = is_.read(buf, offset_, length);
				if (retLen != length)
					return Arrays.copyOf(buf, retLen);
				else
					return buf; 
			} catch (IOException ioe) {
				throw new SQLException(ioe);
			}
		}
		return null;
	}

	// This function populates the Blob data from one of the following:
	// 1. InputStream set in PreparedStatement.setBinaryStream
	// 2. From another clob set in PreparedStatement.setBlob or ResultSet.updateBlob
	// This function is called at the time of PreparedStatement.executeUpdate, execute and 
	// executeBatch

        void populate() throws SQLException
	{
		if (lobWithExternalData_)
			populateFromExternalLob();
		else	
			populateLobData();
	}

	void populateFromExternalLob() throws SQLException
	{
		SQLMXLobOutputStream os;
		if (data_ != null) 
			is_ = new ByteArrayInputStream(data_, offset_, data_.length);
		if (is_ != null) {
			os = (SQLMXLobOutputStream)setOutputStream(1);
			os.populate(is_, length_);
			close();
		} else
			throw Messages.createSQLException(conn_.locale_, "external_lob_has_no_data", null);
	}

	void populateLobData() throws SQLException
	{
		if (JdbcDebugCfg.entryActive) debug[methodId_populate].methodEntry();
		try
		{
			SQLMXLobOutputStream os;
			if (inputLob_ != null) {	
				is_ = inputLob_.getBinaryStream();
			} else if (data_ != null) {
				is_ = new ByteArrayInputStream(data_, offset_, data_.length);
			}
			if (is_ != null) {
				os = (SQLMXLobOutputStream)setOutputStream(1);
				os.populate(is_, length_);
				close();
			}
		}
		finally
		{
			if (JdbcDebugCfg.entryActive) debug[methodId_populate].methodExit();
		}
	}

	static final int findBytes(ByteBuffer buf, int off,  byte ptrn[])
	{
		if (JdbcDebugCfg.entryActive) debug[methodId_findBytes].methodEntry();
		try
		{
			int buf_len = buf.capacity() - off;
			int ptrn_len = ptrn.length;
			int i;					   // index into buf
			int j;					   // index into ptrn;
			byte b = ptrn[0];			// next byte of interest

			for (i = off; i < buf_len; )
			{
				j = 0;
				while (i < buf_len && j < ptrn_len && buf.get() == ptrn[j])
				{
					i++;
					j++;
				}
				if (i == buf_len || j == ptrn_len) return i - j;
				else
				{
					// We have to go back a bit as there may be an overlapping
					// match starting a bit later in buf...
					i = i - j + 1;
				}
			}
			return -1;
		}
		finally
		{
			if (JdbcDebugCfg.entryActive) debug[methodId_findBytes].methodExit();
		}
	}

	byte[] getBytes() throws SQLException 
	{
		return getBytes(1, chunkSize_);
	}

	// Constructors
	public SQLMXBlob(SQLMXConnection conn) throws SQLException 
        {
		super(conn, true);
        }

	public SQLMXBlob(SQLMXStatement stmt, SQLMXDesc lobDesc, String inlineData) throws SQLException
	{
		super(stmt, lobDesc, inlineData, true);
	}

	SQLMXBlob(SQLMXStatement stmt, SQLMXDesc lobDesc, String inlineData, InputStream x, long length) throws SQLException
	{
		super(stmt, lobDesc, inlineData, x, length, true);
	}

	SQLMXBlob(SQLMXStatement stmt, SQLMXDesc lobDesc, String inlineData, Blob inputLob) throws SQLException
	{
		super(stmt, lobDesc, inlineData, true);
		inputLob_ = inputLob;
	}
	
	SQLMXBlob(SQLMXStatement stmt, SQLMXDesc lobDesc, byte[] inlineData, byte[] data) throws SQLException
	{
		super(stmt, lobDesc, inlineData, true, data);
	}

	SQLMXBlob(SQLMXStatement stmt, SQLMXDesc lobDesc, byte[] inlineData) throws SQLException
	{
		super(stmt, lobDesc, inlineData, true, null);
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

	// fields
	private String		traceId_;
	static PrintWriter	traceWriter_;
	static int		traceFlag_;
	Blob			inputLob_;

	private static int methodId_getBinaryStream			=  0;
	private static int methodId_getBytes				=  1;
	private static int methodId_position_LJ				=  2;
	private static int methodId_position_BJ				=  3;
	private static int methodId_setBinaryStream			=  4;
	private static int methodId_setBytes_JB				=  5;
	private static int methodId_setBytes_JBII			=  6;
	private static int methodId_populate				=  7;
	private static int methodId_populateFromBlob		=  8;
	private static int methodId_findBytes				=  9;
	private static int methodId_SQLMXBlob_LLJ			= 10;
	private static int methodId_SQLMXBlob_LLJLI			= 11;
	private static int methodId_SQLMXBlob_LLJL			= 12;
	private static int methodId_SQLMXBlob_LLJB			= 13;
	private static int totalMethodIds					= 14;
	private static JdbcDebug[] debug;
	
	static
	{
		String className = "SQLMXBlob";
		if (JdbcDebugCfg.entryActive)
		{
			debug = new JdbcDebug[totalMethodIds];
			debug[methodId_getBinaryStream] = new JdbcDebug(className,"getBinaryStream");
			debug[methodId_getBytes] = new JdbcDebug(className,"getBytes");
			debug[methodId_position_LJ] = new JdbcDebug(className,"position[LJ]");
			debug[methodId_position_BJ] = new JdbcDebug(className,"position[BJ]");
			debug[methodId_setBinaryStream] = new JdbcDebug(className,"setBinaryStream");
			debug[methodId_setBytes_JB] = new JdbcDebug(className,"setBytes[JB]");
			debug[methodId_setBytes_JBII] = new JdbcDebug(className,"setBytes[JBII]");
			debug[methodId_populate] = new JdbcDebug(className,"populate");
			debug[methodId_populateFromBlob] = new JdbcDebug(className,"populateFromBlob");
			debug[methodId_findBytes] = new JdbcDebug(className,"findBytes");
			debug[methodId_SQLMXBlob_LLJ] = new JdbcDebug(className,"SQLMXBlob[LLJ]");
			debug[methodId_SQLMXBlob_LLJLI] = new JdbcDebug(className,"SQLMXBlob[LLJLI]");
			debug[methodId_SQLMXBlob_LLJL] = new JdbcDebug(className,"SQLMXBlob[LLJL]");
			debug[methodId_SQLMXBlob_LLJB] = new JdbcDebug(className,"SQLMXBlob[LLJB]");
		}
	}

        public void free() throws SQLException {
		close();
        }

        public InputStream getBinaryStream() throws SQLException {
                return getBinaryStream(1, Long.MAX_VALUE);
        }

}
