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
 * Filename		: SQLMXLobInputStream.java
 * Description	: This program implements the InputStream interface.
 *      This object returned to the application when Clob.getInputStream()
 *      method or Blob.getInputStream is called. The application can use
 *      this object to read the clob/blob data
 *
 */

package org.apache.trafodion.jdbc.t2;

import java.sql.*;
import java.io.InputStream;
import java.io.IOException;
import java.util.Date;
import java.io.PrintWriter;
import java.nio.ByteBuffer;

public class SQLMXLobInputStream extends InputStream
{
	public int available() throws IOException
	{
		int remainLen;

		remainLen = bytesRead_ - currentPos_;
		if (remainLen == 0) // 0 would mean all the bytes are read from chunk_
			remainLen = lob_.chunkSize_;
		return remainLen;
	}

	public void close() throws IOException
	{
		isClosed_ = true;
	}

	public void mark(int readlimit)
	{
	}

	public boolean markSupported()
	{
		return false;
	}

	public int read() throws IOException
	{	
		if (JdbcDebugCfg.entryActive) debug[methodId_read_V].methodEntry();
		try
		{
			int retValue;

			if (currentPos_ == bytesRead_) {
				retValue = readChunkThrowIO();
				if (retValue == -1)
					return retValue;
			}
			retValue = chunk_.get();
			currentPos_++;
			return retValue;
		}
		finally
		{
			if (JdbcDebugCfg.entryActive) debug[methodId_read_V].methodExit();
		}
	}

	public int read(byte[] b) throws IOException
	{
		return read(b, 0, b.length);
	}

	public int read(byte[] b, int off, int len) throws IOException
	{
		return read(b, off, len, false);
	}

	public int read(byte[] b, int off, int len, boolean skip) throws IOException
	{
		if (JdbcDebugCfg.entryActive) debug[methodId_read_BII].methodEntry();
		try
		{
			int remainLen;
			int copyLen;
			int copyOffset;
			int retLen;
			int availableLen;
			int copiedLen = 0;

			if (b == null && (! skip))
				throw new IOException("Invalid input value");
			remainLen = len;
			copyOffset = off;
			
			while (remainLen > 0) {
				availableLen = bytesRead_ - currentPos_;
				if (availableLen > remainLen)	
					copyLen = remainLen;
				else
					copyLen = availableLen;
				if (copyLen > 0) {
					if (! skip)
						chunk_.get(b, copyOffset, copyLen);			
					else
						chunk_.position(currentPos_+copyLen-1);
					currentPos_ += copyLen;
					copyOffset += copyLen;
					copiedLen += copyLen;
					remainLen -= copyLen;
		
				}
				if (remainLen > 0) {
					retLen = readChunkThrowIO();
					if (retLen == -1)
						break;
				}
			}
			return copiedLen;
		}		
		finally
		{
			if (JdbcDebugCfg.entryActive) debug[methodId_read_BII].methodExit();
		}
	}

	public void reset() throws IOException
	{
		if (JdbcDebugCfg.entryActive) debug[methodId_reset].methodEntry();
		try
		{
			currentPos_ = 0;
			bytesRead_ = 0;
			currentLobPos_ = 0;
			return;
		}
		finally
		{
			if (JdbcDebugCfg.entryActive) debug[methodId_reset].methodExit();
		}
	}

	public long skip(long n) throws IOException
	{
		long totalSkippedLen = 0;
		long skipRemain = n;
		int skipLen;
		int skippedLen;
		while (skipRemain > 0) {
			if (skipRemain <= Integer.MAX_VALUE)
				skipLen = (int)skipRemain;
			else
				skipLen = Integer.MAX_VALUE;	
			skippedLen = read(null, 0, skipLen, true); 
			if (skippedLen == -1)
				break;
			skipRemain -= skippedLen;
			totalSkippedLen += skippedLen;
		}	
		return totalSkippedLen;
	}

	int readChunkThrowIO() throws IOException
	{
		int readLen = -1;
		try {
			if (startLobPos_ <= currentLobPos_)
				readLen = readChunk();
			else {
				while (startLobPos_ > currentLobPos_) 
					readLen = readChunk();
				long prevStartLobPos = currentLobPos_ - readLen;
				currentPos_ = (int)(startLobPos_ - prevStartLobPos);
			}
		}
		catch (SQLException e)
		{
			throw new IOException(SQLMXLob.convSQLExceptionToIO(e));
		}
		return readLen;
	}
	
	int readChunk() throws SQLException
	{
		// eos_ can be set to true before all the bytes are read from chunk
		// So, don't use eos_ in any other function
		int extractMode = 1; // get the lob data
		if (eos_)
			return -1;
		int readLen;
		// limit the read by the required length_
		if (currentLobPos_ >= startLobPos_) {
			if (length_ > (currentLobPos_ + lob_.chunkSize_))
				readLen = lob_.chunkSize_;
			else
				readLen = (int) (length_ - currentLobPos_); 
		} else 
			readLen  = lob_.chunkSize_;
		if (readLen == 0) {
			eos_ = true;
			return -1;
		}	
		do {
			long tmpCurrentLobPos = currentLobPos_;
			chunk_ = readChunk(conn_.server_, conn_.getDialogueId(), stmt_.getStmtId(), conn_.getTxid(), extractMode, 
				lob_.lobVersion_, lob_.getLobTableUid(), lob_.lobLocator_, readLen); 
			if (chunk_ != null && (bytesRead_ = chunk_.capacity()) > 0) {
				if (tmpCurrentLobPos > startLobPos_)
					currentPos_ = 0;
				else if ((tmpCurrentLobPos + bytesRead_) > startLobPos_)
					currentPos_ = (int)(startLobPos_ - tmpCurrentLobPos);
				else	
					currentPos_ = bytesRead_;
				currentLobPos_ += bytesRead_;
				if (bytesRead_ < readLen)
					eos_ = true;
			} else {
				bytesRead_ = -1;
				eos_ = true;
				break;
			}
				
		} while ((currentLobPos_ < startLobPos_) && (! eos_)); 
		return bytesRead_;
	}

	long getBeginLobPos() 
	{
		return currentLobPos_- bytesRead_;
	}

        native ByteBuffer readChunk(String server, long dialogueId, long stmtId, long txid, int extractMode, short lobVersion, 
				long lobTableUid, String lobLocator, int chunkLen);

	// Constructor
	SQLMXLobInputStream(SQLMXStatement stmt, SQLMXConnection conn, SQLMXLob lob, long startPos, long length)
	{
		if (JdbcDebugCfg.entryActive) debug[methodId_SQLMXLobInputStream].methodEntry();
		try
		{
			lob_ = lob;
			stmt_ = stmt;	
			conn_ = conn;
			chunk_ = null;
			bytesRead_ = 0;
			currentPos_ = 0;
			startLobPos_ = startPos;
			currentLobPos_ = 1;
			if (length < (Long.MAX_VALUE - startLobPos_))
				length_  = length + startLobPos_;
			else
				length_ = length;
			eos_ = false;
			isClosed_ = false;
			traceWriter_ = SQLMXDataSource.traceWriter_;
			if (lob_.inlineDataBytes_ != null) {
				int inlineLen = lob_.inlineDataBytes_.length;
				currentLobPos_ = inlineLen;
				if (inlineLen > startLobPos_) {
					chunk_ = ByteBuffer.wrap(lob_.inlineDataBytes_, (int)startLobPos_-1, 
						inlineLen - (int)startLobPos_ + 1);
					bytesRead_ = inlineLen;
					currentPos_ = (int)startLobPos_-1;
				}
			}
		}
		finally
		{
			if (JdbcDebugCfg.entryActive) debug[methodId_SQLMXLobInputStream].methodExit();
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
	SQLMXLob		lob_;
	SQLMXStatement		stmt_;
	SQLMXConnection		conn_;
	boolean			isClosed_;
	ByteBuffer		chunk_;
	int			currentPos_;
	int			bytesRead_;
	boolean 		eos_;
	long			startLobPos_;
	long			currentLobPos_;
	long			length_;

	private static int methodId_available			=  0;
	private static int methodId_close				=  1;
	private static int methodId_mark				=  2;
	private static int methodId_markSupported		=  3;
	private static int methodId_read_V				=  4;
	private static int methodId_read_B				=  5;
	private static int methodId_read_BII			=  6;
	private static int methodId_reset				=  7;
	private static int methodId_skip				=  8;
	private static int methodId_readChunkThrowIO	=  9;
	private static int methodId_readChunk			= 10;
	private static int methodId_SQLMXLobInputStream	= 11;
	private static int totalMethodIds				= 12;
	private static JdbcDebug[] debug;
	
	static
	{
		String className = "SQLMXLobInputStream";
		if (JdbcDebugCfg.entryActive)
		{
			debug = new JdbcDebug[totalMethodIds];
			debug[methodId_available] = new JdbcDebug(className,"available");
			debug[methodId_close] = new JdbcDebug(className,"close");
			debug[methodId_markSupported] = new JdbcDebug(className,"markSupported");
			debug[methodId_read_V] = new JdbcDebug(className,"read[V]");
			debug[methodId_read_B] = new JdbcDebug(className,"read[B]");
			debug[methodId_read_BII] = new JdbcDebug(className,"read[BII]");
			debug[methodId_reset] = new JdbcDebug(className,"reset");
			debug[methodId_skip] = new JdbcDebug(className,"skip");
			debug[methodId_readChunkThrowIO] = new JdbcDebug(className,"readChunkThrowIO");
			debug[methodId_readChunk] = new JdbcDebug(className,"readChunk");
			debug[methodId_SQLMXLobInputStream] = new JdbcDebug(className,"SQLMXLobInputStream");
		}
	}
}
