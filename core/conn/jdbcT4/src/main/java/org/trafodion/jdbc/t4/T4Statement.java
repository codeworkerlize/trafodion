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

package org.trafodion.jdbc.t4;

import java.nio.charset.CharacterCodingException;
import java.nio.charset.UnsupportedCharsetException;
import java.sql.SQLException;
import org.trafodion.jdbc.t4.trace.TraceQueryUsedTime;

final class T4Statement {
    private int m_queryTimeout;
    private String m_stmtLabel;
    private String m_stmtExplainLabel;
    private static short EXTERNAL_STMT = 0;
    private InterfaceConnection m_ic;
    private T4Connection m_serverConnection;
    private long prepareTimeMills = 0L;
    private boolean m_processing = false;

	// -----------------------------------------------------------------------------------
	T4Statement(InterfaceStatement is) throws SQLException {
                m_ic = is.ic_;
		m_serverConnection = m_ic.getT4Connection();
		m_stmtLabel = is.getStmtLabel();
		m_stmtExplainLabel = "";

		if (m_stmtLabel == null) {
		 	m_serverConnection.throwInternalException();
		}
	}// end T4Statement

	// -----------------------------------------------------------------------------------

	ExecuteReply Execute(short executeAPI, int sqlAsyncEnable, int inputRowCnt, int maxRowsetSize, int sqlStmtType,
			int stmtHandle, String sqlString, int sqlStringCharset, String cursorName, int cursorNameCharset,
			String stmtLabel, int stmtLabelCharset, SQL_DataValue_def inputDataValue, SQLValueList_def inputValueList,
			byte[] txId, boolean userBuffer,int executeApiType) throws SQLException {
		try {
			//m_serverConnection.getInputOutput().setTimeout(m_queryTimeout);

			LogicalByteArray wbuffer = ExecuteMessage.marshal(m_ic.getDialogueId(), sqlAsyncEnable, this.m_queryTimeout,
					inputRowCnt, maxRowsetSize, sqlStmtType, stmtHandle, EXTERNAL_STMT, sqlString,
					sqlStringCharset, cursorName, cursorNameCharset, stmtLabel, stmtLabelCharset,
					this.m_stmtExplainLabel, inputDataValue, inputValueList, txId, userBuffer, this.m_ic,executeApiType);
            LogicalByteArray rbuffer;
			if (!this.m_ic.getTrafT4Conn().isBatchFlag()) {
			    long start = 0;
			    if (this.m_ic.getT4props().isTraceTransTime() && !this.m_ic.getAutoCommit()) {
			        start = System.currentTimeMillis();
			        this.m_ic.getTrafT4Conn().checkTransStartTime(start);
			    }
			    rbuffer = getReadBuffer(executeAPI, wbuffer);
			    this.m_ic.getTrafT4Conn().addTransactionUsedTimeList(stmtLabel, sqlString, start,
			    TRANSPORT.TRACE_EXECUTE_TIME);
			} else {
			    rbuffer = getReadBuffer(executeAPI, wbuffer);
			}
			ExecuteReply er = new ExecuteReply(rbuffer, m_ic);

			return er;
		} catch (SQLException e) {
			throw e;
		} catch (CharacterCodingException e) {
			SQLException se = TrafT4Messages.createSQLException(m_ic.getT4props(),
					"translation_of_parameter_failed", "ExecuteMessage", e.getMessage());
			se.initCause(e);
			throw se;
		} catch (UnsupportedCharsetException e) {
			SQLException se = TrafT4Messages.createSQLException(m_ic.getT4props(), "unsupported_encoding", e
					.getCharsetName());
			se.initCause(e);
			throw se;
		} catch (Exception e) {
			SQLException se = TrafT4Messages.createSQLException(m_ic.getT4props(), "execute_message_error", e
					.getMessage());
			se.initCause(e);
			throw se;
		}
	} // end Execute

	// -----------------------------------------------------------------------------------
	GenericReply ExecuteGeneric(short executeAPI, byte[] messageBuffer) throws SQLException {
		LogicalByteArray wbuffer = null;
		LogicalByteArray rbuffer = null;
		GenericReply gr = null;

		try {
			//set time out is the maximum value of int
			m_serverConnection.getInputOutput().setTimeout(0);
			wbuffer = GenericMessage.marshal(messageBuffer, this.m_ic);
			rbuffer = getReadBuffer(executeAPI, wbuffer);
			gr = new GenericReply(rbuffer);

			return gr;
		} catch (SQLException se) {
			throw se;
		} catch (Exception e) {
			SQLException se = TrafT4Messages.createSQLException(m_ic.getT4props(), "execute_message_error", e
					.getMessage());

			se.initCause(e);
			throw se;
		}
	} // end ExecuteGeneric

	// -----------------------------------------------------------------------------------
	PrepareReply Prepare(int sqlAsyncEnable, short stmtType, int sqlStmtType, String stmtLabel, int stmtLabelCharset,
			String cursorName, int cursorNameCharset, String moduleName, int moduleNameCharset, long moduleTimestamp,
			String sqlString, int sqlStringCharset, String stmtOptions, int maxRowsetSize, byte[] txId

	) throws SQLException {

		if (sqlString == null) {
			m_serverConnection.throwInternalException();
		}
		try {
			////set time out is the maximum value of int
			m_serverConnection.getInputOutput().setTimeout(0);

			LogicalByteArray wbuffer = PrepareMessage.marshal(m_serverConnection.getDialogueId(), sqlAsyncEnable, this.m_queryTimeout,
					stmtType, sqlStmtType, stmtLabel, stmtLabelCharset, cursorName, cursorNameCharset, moduleName,
					moduleNameCharset, moduleTimestamp, sqlString, sqlStringCharset, stmtOptions,
					this.m_stmtExplainLabel, maxRowsetSize, txId, this.m_ic);
			LogicalByteArray rbuffer;
            if (this.m_ic.getT4props().isTraceTransTime() && !this.m_ic.getAutoCommit()) {
				long start = System.currentTimeMillis();
                this.m_ic.getTrafT4Conn().checkTransStartTime(start);
				rbuffer = getReadBuffer(TRANSPORT.SRVR_API_SQLPREPARE, wbuffer);
				this.m_ic.getTrafT4Conn().addTransactionUsedTimeList(stmtLabel, sqlString, start, TRANSPORT.TRACE_PREPARE_TIME);
				prepareTimeMills = System.currentTimeMillis() - start;
            } else {
				rbuffer = getReadBuffer(TRANSPORT.SRVR_API_SQLPREPARE, wbuffer);
			}
			PrepareReply pr = new PrepareReply(rbuffer, m_ic);

			return pr;
		} catch (SQLException se) {
			throw se;
		} catch (CharacterCodingException e) {
			SQLException se = TrafT4Messages.createSQLException(m_ic.getT4props(),
					"translation_of_parameter_failed", "PrepareMessage", e.getMessage());
			se.initCause(e);
			throw se;
		} catch (UnsupportedCharsetException e) {
			SQLException se = TrafT4Messages.createSQLException(m_ic.getT4props(), "unsupported_encoding", e
					.getCharsetName());
			se.initCause(e);
			throw se;
		} catch (Exception e) {
			SQLException se = TrafT4Messages.createSQLException(m_ic.getT4props(), "prepare_message_error", e
					.getMessage());

			se.initCause(e);
			throw se;
		}
	} // end Prepare

	// -----------------------------------------------------------------------------------

	CloseReply Close() throws SQLException {
		try {
			LogicalByteArray wbuffer = CloseMessage.marshal(m_serverConnection.getDialogueId(), m_stmtLabel, InterfaceStatement.SQL_DROP,
					this.m_ic);

			//set time out is the maximum value of int
			m_serverConnection.getInputOutput().setTimeout(0);

			LogicalByteArray rbuffer = getReadBuffer(TRANSPORT.SRVR_API_SQLFREESTMT, wbuffer);

			CloseReply cr = new CloseReply(rbuffer, m_serverConnection.getNCSAddress().getIPorName(), m_ic);

			return cr;
		} catch (SQLException se) {
			throw se;
		} catch (CharacterCodingException e) {
			SQLException se = TrafT4Messages.createSQLException(m_ic.getT4props(),
					"translation_of_parameter_failed", "CloseMessage", e.getMessage());
			se.initCause(e);
			throw se;
		} catch (UnsupportedCharsetException e) {
			SQLException se = TrafT4Messages.createSQLException(m_ic.getT4props(), "unsupported_encoding", e
					.getCharsetName());
			se.initCause(e);
			throw se;
		} catch (Exception e) {
			SQLException se = TrafT4Messages.createSQLException(m_ic.getT4props(), "close_message_error", e
					.getMessage());

			se.initCause(e);
			throw se;
		}
	}

	// --------------------------------------------------------------------------------
	protected LogicalByteArray getReadBuffer(short odbcAPI, LogicalByteArray wbuffer) throws SQLException {
		LogicalByteArray buf = null;

		try {
			m_processing = true;
			m_ic.setProcessing(true);
			buf = m_serverConnection.getReadBuffer(odbcAPI, wbuffer);
			m_processing = false;
			m_ic.setProcessing(false);
		} catch (SQLException se) {
			m_processing = false;
			m_ic.setProcessing(false);
			throw se;
		}
		return buf;
	}

    public void setQueryTimeout(int queryTimeout) {
        this.m_queryTimeout = queryTimeout;
    }

    public boolean isProcessing() {
        return m_processing;
    }

    public long getPrepareTimeMills() {
        return prepareTimeMills;
    }
    
    protected void updateConnContext(InterfaceConnection ic){
        this.m_ic = ic;
        this.m_serverConnection = ic.getT4Connection();
    }
}
