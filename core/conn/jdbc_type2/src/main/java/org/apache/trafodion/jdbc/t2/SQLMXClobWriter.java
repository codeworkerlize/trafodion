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
 * Filename    : SQLMXClobWriter.java
 * Description : This program implements the Writer interface. This
 *      object returned to the application when Clob.setAsciiStream() 
 *      method is called. The application can use this object to write 
 *      the clob data
 */

package org.apache.trafodion.jdbc.t2;

import java.sql.*;
import java.io.Writer;
import java.io.Reader;
import java.io.IOException;
import java.util.Date;
import java.nio.charset.Charset;
import java.io.PrintWriter;

public class SQLMXClobWriter extends Writer
{
	public void close() throws IOException
	{
		if (JdbcDebugCfg.entryActive) debug[methodId_close].methodEntry();
		try
		{
			if (! isClosed_)
			{
				flush();
				isClosed_ = true;
			}
		}
		finally
		{
			if (JdbcDebugCfg.entryActive) debug[methodId_close].methodExit();
		}
	}

	public void flush() throws IOException
	{
		if (JdbcDebugCfg.entryActive) debug[methodId_flush].methodEntry();
		try
		{
			if (isClosed_)
				throw new IOException("Output stream is in closed state");
			if (! isFlushed_)
				writeChunkThrowIO(chunk_, 0, currentPos_);
		}
		finally
		{
			if (JdbcDebugCfg.entryActive) debug[methodId_flush].methodExit();
		}
	}

	public void write(char[] cbuf) throws IOException
	{
		if (JdbcDebugCfg.entryActive) debug[methodId_write_C].methodEntry();
		try
		{
			if (cbuf == null)
				throw new IOException("Invalid input value");
			write(cbuf, 0, cbuf.length);
		}
		finally
		{
			if (JdbcDebugCfg.entryActive) debug[methodId_write_C].methodExit();
		}
	}

	public void write(char[] cbuf, int off, int len) throws IOException
	{
		if (JdbcDebugCfg.entryActive) debug[methodId_write_CII].methodEntry();
		try
		{
			int copyLen;
			int	srcOffset;
			int tempLen;

			if (isClosed_)
				throw new IOException("Writer is in closed state");
			if (cbuf == null)
				throw new IOException("Invalid input value");
			if (off < 0 || len < 0 || off > cbuf.length)
				throw new IndexOutOfBoundsException(
					"length or offset is less than 0 or offset is greater than the length of array");
			srcOffset = off;
			copyLen = len;
			while (true) {
				if ((copyLen+currentPos_) < (clob_.chunkSize_)) {
					System.arraycopy(cbuf, srcOffset, chunk_, currentPos_, copyLen);
					currentPos_ += copyLen;
					isFlushed_ = false;
					break;
				} else {
					if (currentPos_ != 0) {
						tempLen = clob_.chunkSize_-currentPos_;
						System.arraycopy(cbuf, srcOffset, chunk_, currentPos_, tempLen);
						currentPos_ += tempLen;
						writeChunkThrowIO(chunk_, 0, currentPos_);
					} else {
						tempLen = clob_.chunkSize_;
						writeChunkThrowIO(cbuf, srcOffset, tempLen);
					}
					copyLen -= tempLen;
					srcOffset += tempLen;
				}
			}
		}
		finally
		{
			if (JdbcDebugCfg.entryActive) debug[methodId_write_CII].methodExit();
		}
	}
	
	public void write(int c) throws IOException
	{
		if (JdbcDebugCfg.entryActive) debug[methodId_write_I].methodEntry();
		try
		{
			if (isClosed_)
				throw new IOException("Writer is in closed state");
			chunk_[currentPos_] = (char)c;
			isFlushed_ = false;
			currentPos_++;
			if (currentPos_ == clob_.chunkSize_) {
				writeChunkThrowIO(chunk_, 0, currentPos_);
			}
		}
		finally
		{
			if (JdbcDebugCfg.entryActive) debug[methodId_write_I].methodExit();
		}
	}

	public void write(String str) throws IOException
	{
		if (JdbcDebugCfg.entryActive) debug[methodId_write_L].methodEntry();
		try
		{
			if (str == null)
				throw new IOException("Invalid input value");
			write(str, 0, str.length());
		}
		finally
		{
			if (JdbcDebugCfg.entryActive) debug[methodId_write_L].methodExit();
		}
	}

	public void write(String str, int off, int len) throws IOException
	{
		if (JdbcDebugCfg.entryActive) debug[methodId_write_LII].methodEntry();
		try
		{
			int copyLen;
			int srcOffset;
			int tempLen;

			if (isClosed_)
				throw new IOException("Output stream is in closed state");
			if (str == null)
				throw new IOException("Invalid input value");
			if (off < 0 || len < 0 || off > str.length())
				throw new IndexOutOfBoundsException(
					"length or offset is less than 0 or offset is greater than the length of array");
			srcOffset = off;
			copyLen = len;
			while (true) {
				if ((copyLen+currentPos_) < clob_.chunkSize_) {
					System.arraycopy(str, srcOffset, chunk_, currentPos_, copyLen);
					currentPos_ += copyLen;
					isFlushed_ = false;
					break;
				} else {
					if (currentPos_ != 0) {
						tempLen = clob_.chunkSize_-currentPos_;		
						System.arraycopy(str, srcOffset, chunk_, currentPos_, tempLen);
						currentPos_ += tempLen;
						writeChunkThrowIO(chunk_, 0, currentPos_);
					} else {
						tempLen = clob_.chunkSize_;
						writeChunkThrowIO(str.toCharArray(), srcOffset, tempLen);
					}	
					copyLen -= tempLen;
					srcOffset += tempLen;
				}
				
			}
		}
		finally
		{
			if (JdbcDebugCfg.entryActive) debug[methodId_write_LII].methodExit();
		}
	}

	void writeChunkThrowIO(char[] chunk, int offset, int len) throws IOException
	{
		if (JdbcDebugCfg.entryActive) debug[methodId_writeChunkThrowIO].methodEntry();
		try
		{
			try
			{
				writeChunk(chunk, offset, len);
			}
			catch (SQLException e)
			{
				throw new IOException(SQLMXLob.convSQLExceptionToIO(e));
			}
		}
		finally
		{
			if (JdbcDebugCfg.entryActive) debug[methodId_writeChunkThrowIO].methodExit();
		}
	}

	void writeChunk(char[] chunk, int offset, int len) throws SQLException
	{
		if (len == 0) {
			isFlushed_ = true;
			return;
		}
		long lobWritePos = -1;
		if (currentChunkNo_  == 0) {
			if (startingPos_ == 1  && stmt_.getSqlType() ==  SQLMXConnection.TYPE_INSERT_PARAM 
				|| stmt_.getSqlType() == SQLMXConnection.TYPE_INSERT)
				lobWritePos = -1;
			else
				lobWritePos = startingPos_;
		}
		String chunkStr;
		if (len == chunk.length && offset == 0)
			chunkStr = new String(chunk);
		else
			chunkStr = new String(chunk, offset, len);
		writeChunk(conn_.server_, conn_.getDialogueId(), stmt_.getStmtId(), conn_.getTxid(),
				clob_.lobVersion_, clob_.getLobTableUid(), clob_.lobLocator_, 
				chunkStr.getBytes(Charset.forName(SQLMXDesc.getCharacterSetName(clob_.sqlCharSet_))), lobWritePos);

		currentChunkNo_++;
		currentPos_ = 0;
		isFlushed_ = true;
 	}

	native void writeChunk(String server, long dialogueId, long stmtId, long txid, short lobVersion, long lobTableUid, 
			String lobLocator, byte[] chunk, long pos);
	
	void populate(Reader ir, long length) throws SQLException
	{
		if (JdbcDebugCfg.entryActive) debug[methodId_populate].methodEntry();
		try
		{
			int tempLen;
			long readLen;
			int retLen;
				
			readLen = length;
			try
			{
				while (true)
				{
					if (readLen > 0 && readLen <= clob_.chunkSize_)
						tempLen = (int)readLen;
					else
						tempLen = clob_.chunkSize_;
					if (tempLen == 0)
						break;
					retLen = ir.read(chunk_, 0, tempLen);
					if (retLen == -1 || retLen == 0 || (length != 0 && readLen == 0))
						break;
					writeChunk(chunk_, 0, retLen);
					if (length > 0)
						readLen -= retLen;
				}
			}
			catch (IOException e)
			{
				Object[] messageArguments = new Object[1];
				messageArguments[0] = e.getMessage();
				throw Messages.createSQLException(conn_.locale_, "io_exception", 
					messageArguments);
			}
		}
		finally
		{
			if (JdbcDebugCfg.entryActive) debug[methodId_populate].methodExit();
		}
	}

	// constructors
	SQLMXClobWriter(SQLMXStatement stmt, SQLMXConnection conn, SQLMXClob clob, long pos) throws SQLException
	{
		if (JdbcDebugCfg.entryActive) debug[methodId_SQLMXClobWriter].methodEntry();
		try
		{
			clob_ = clob;
			stmt_ = stmt;
			conn_ = conn;
			chunk_ = new char[clob_.chunkSize_];
			isFlushed_ = false;
			startingPos_ = pos;
			currentPos_ = 0;
			currentChunkNo_ = 0;
			traceWriter_ = SQLMXDataSource.traceWriter_;
		}
		finally
		{
			if (JdbcDebugCfg.entryActive) debug[methodId_SQLMXClobWriter].methodExit();
		}
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

	// Fields
	private String		traceId_;
	static PrintWriter	traceWriter_;
	static int		traceFlag_;
	SQLMXClob		clob_;
	long			startingPos_;
	SQLMXStatement		stmt_;
	SQLMXConnection		conn_;
	boolean			isClosed_;
	char[]			chunk_;
	int			currentPos_;
	int			currentChunkNo_;
	boolean			isFlushed_;

	private static int methodId_close				=  0;
	private static int methodId_flush				=  1;
	private static int methodId_write_C				=  2;
	private static int methodId_write_CII			=  3;
	private static int methodId_write_I				=  4;
	private static int methodId_write_L				=  5;
	private static int methodId_write_LII			=  6;
	private static int methodId_writeChunk			=  7;
	private static int methodId_writeChunkThrowIO	=  8;
	private static int methodId_populate			=  9;
	private static int methodId_SQLMXClobWriter		= 10;
	private static int totalMethodIds				= 11;
	private static JdbcDebug[] debug;
	
	static
	{
		String className = "SQLMXClobWriter";
		if (JdbcDebugCfg.entryActive)
		{
			debug = new JdbcDebug[totalMethodIds];
			debug[methodId_close] = new JdbcDebug(className,"close");
			debug[methodId_flush] = new JdbcDebug(className,"flush");
			debug[methodId_write_C] = new JdbcDebug(className,"write[C]");
			debug[methodId_write_CII] = new JdbcDebug(className,"write[CII]");
			debug[methodId_write_I] = new JdbcDebug(className,"write[I]");
			debug[methodId_write_L] = new JdbcDebug(className,"write[L]");
			debug[methodId_write_LII] = new JdbcDebug(className,"write[LII]");
			debug[methodId_writeChunk] = new JdbcDebug(className,"writeChunk");
			debug[methodId_writeChunkThrowIO] = new JdbcDebug(className,"writeChunkThrowIO");
			debug[methodId_populate] = new JdbcDebug(className,"populate");
			debug[methodId_SQLMXClobWriter] = new JdbcDebug(className,"SQLMXClobWriter");
		}
	}
}
