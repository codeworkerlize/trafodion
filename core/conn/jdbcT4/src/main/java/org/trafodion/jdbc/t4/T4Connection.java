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
import java.util.logging.Level;
import org.slf4j.LoggerFactory;
import org.trafodion.jdbc.t4.trace.TraceQueryUsedTime;

class T4Connection {
	InputOutput m_io;
	InterfaceConnection m_ic;

	static final int INCONTEXT_OPT1_SESSIONNAME = 0x80000000; // (2^31)
	static final int INCONTEXT_OPT1_FETCHAHEAD = 0x40000000; // (2^30)
	static final int INCONTEXT_OPT1_CERTIFICATE_TIMESTAMP = 0x20000000; //(2^29)
	static final int INCONTEXT_OPT1_CLIENT_USERNAME = 0x10000000; //(2^28)
	
	private final static org.slf4j.Logger LOG = LoggerFactory.getLogger(T4Connection.class);

	T4Connection(InterfaceConnection ic) throws SQLException {
		if (ic == null) {
			throwInternalException();
		}
		m_ic = ic;

		if (ic.getDialogueId() < 1 || m_ic.getNCSAddress() == null || ic.getUserDescription() == null || ic.getInContext() == null) {
			throwInternalException();
		}
		m_io = new InputOutput(ic.getT4props(), ic.getNCSAddress());

		m_io.setDialogueId(ic.getDialogueId());
		m_io.setConnectionIdleTimeout(ic.getConnectionTimeout());
		m_io.setInterfaceConnection(this.m_ic);
		m_io.setNetworkTimeoutInMillis(ic.getT4props().getNetworkTimeoutInMillis());
		m_io.openIO();
		getInputOutput().setTimeout(ic.getLoginTimeout());
	}

	public void finalizer() {
		closeTimers();
	}

	protected int getDialogueId() {
		return m_ic.getDialogueId();
	}

	protected String getSessionName() {
		return this.m_ic.getSessionName();
	}

	protected NCSAddress getNCSAddress() {
		return m_ic.getNCSAddress();
	}

	void closeTimers() {
		if (m_io != null) {
			m_io.closeTimers();
		}
	}

	protected void reuse() {
		resetConnectionIdleTimeout();
	}

	private void setConnectionIdleTimeout() {
		m_io.startConnectionIdleTimeout();
	}

	private void resetConnectionIdleTimeout() {
		m_io.resetConnectionIdleTimeout();
	}

	private void checkConnectionIdleTimeout() throws SQLException {
		if (m_io.checkConnectionIdleTimeout()) {
			try {
				m_ic.close();
			} catch (SQLException sqex) {
				// ignores
			}
			throw TrafT4Messages.createSQLException(getT4props(), "ids_s1_t00", m_ic.getTargetServer(), m_ic.getConnectionTimeout());
		}
	}

	protected boolean connectionIdleTimeoutOccured() {
		return m_io.checkConnectionIdleTimeout();
	}

	protected InputOutput getInputOutput() throws SQLException {
		checkConnectionIdleTimeout();
		resetConnectionIdleTimeout();
		return m_io;
	}

	protected void throwInternalException() throws SQLException {
		T4Properties tempP = null;

		if (m_ic != null) {
			tempP = getT4props();

		}
		SQLException se = TrafT4Messages.createSQLException(tempP, "internal_error");
		SQLException se2 = TrafT4Messages.createSQLException(tempP, "contact_traf_error");

		se.setNextException(se2);
		throw se;
	}

	// --------------------------------------------------------------------------------
    protected LogicalByteArray getReadBuffer(short odbcAPI, LogicalByteArray wbuffer)
            throws SQLException {
        // trace_connection - AM
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", odbcAPI);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", odbcAPI);
        }
        LogicalByteArray rbuffer = m_io.doIO(odbcAPI, wbuffer);
        
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "LEAVE", odbcAPI);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("LEAVE. {}", odbcAPI);
        }
        return rbuffer;
    }

    private T4Properties getT4props() {
        return m_ic.getT4props();
    }

	// --------------------------------------------------------------------------------
	/**
	 * This class corresponds to the ODBC client driver function
	 * odbc_SQLSvc_InitializeDialogue_pst_ as taken from odbccs_drvr.cpp.
	 * @version 1.0
	 * 
	 * This method will make a connection to an ODBC server. The ODBC server's
	 * locaiton This method will make a connection to an ODBC server. The ODBC
	 * server's locaiton (i.e. ip address and port number), were provided by an
	 * earlier call to the ODBC association server.
	 * 
	 * @param inContext
	 *            a CONNETION_CONTEXT_def object containing connection
	 *            information
	 * @param userDesc
	 *            a USER_DESC_def object containing user information
	 * @param inContext
	 *            a CONNECTION_CONTEXT_def object containing information for
	 *            this connection
	 * @param dialogueId
	 *            a unique id identifing this connection as supplied by an
	 *            earlier call to the association server
	 * 
	 * @retrun a InitializeDialogueReply class representing the reply from the
	 *         ODBC server is returned
	 * 
	 * @exception A
	 *                SQLException is thrown
	 */

	InitializeDialogueReply InitializeDialogue(boolean setTimestamp, boolean downloadCert) throws SQLException {
		try {
			int optionFlags1 = INCONTEXT_OPT1_CLIENT_USERNAME;
			int optionFlags2 = 0;
			
			if(setTimestamp) {
				optionFlags1 |= INCONTEXT_OPT1_CERTIFICATE_TIMESTAMP;
			}

			if (m_ic.getSessionName() != null && m_ic.getSessionName().length() > 0) {
				optionFlags1 |= INCONTEXT_OPT1_SESSIONNAME;
			}

			if (this.m_ic.getT4props().getFetchAhead()) {
				optionFlags1 |= INCONTEXT_OPT1_FETCHAHEAD;
			}

			// trace_connection - AM
            if (getT4props().isLogEnable(Level.FINER)) {
                String temp = "m_dialogueId=" + m_ic.getDialogueId();
                T4LoggingUtilities.log(getT4props(), Level.FINER, temp);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("m_dialogueId= {}", m_ic.getDialogueId());
            }
			LogicalByteArray wbuffer = InitializeDialogueMessage.marshal(m_ic.getUserDescription(), m_ic.getInContext(), m_ic.getDialogueId(),
					optionFlags1, optionFlags2, m_ic.getSessionName(), m_ic);

			getInputOutput().setTimeout(m_ic.getLoginTimeout());

			LogicalByteArray rbuffer = getReadBuffer(TRANSPORT.SRVR_API_SQLCONNECT, wbuffer);

			//
			// Process output parameters
			//
			InitializeDialogueReply idr1 = new InitializeDialogueReply(rbuffer, m_ic.getNCSAddress().getIPorName(), m_ic, downloadCert);

			return idr1;
		} catch (SQLException se) {
			throw se;
		} catch (CharacterCodingException e) {
			SQLException se = TrafT4Messages.createSQLException(getT4props(), 
					"translation_of_parameter_failed", "InitializeDialogueMessage", e.getMessage());
			se.initCause(e);
			throw se;
		} catch (UnsupportedCharsetException e) {
			SQLException se = TrafT4Messages.createSQLException(getT4props(), "unsupported_encoding", e
					.getCharsetName());
			se.initCause(e);
			throw se;
		} catch (Exception e) {
			SQLException se = TrafT4Messages.createSQLException(getT4props(), 
					"initialize_dialogue_message_error", e.getMessage());

			se.initCause(e);
			throw se;
		} // end catch
	} // end InitializeDialogue

	/**
	 * This method will end a connection to an ODBC server. The ODBC server's
	 * locaiton (i.e. ip address and port number), were provided by an earlier
	 * call to the ODBC association server.
	 * 
	 * @retrun a TerminateDialogueReply class representing the reply from the
	 *         ODBC server is returned
	 * 
	 * @exception A
	 *                SQLException is thrown
	 */
	TerminateDialogueReply TerminateDialogue() throws SQLException {
		try {
			// trace_connection - AM
			if (getT4props().isLogEnable(Level.FINEST)) {
			    T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
			}
			if (LOG.isTraceEnabled()) {
			    LOG.trace("ENTRY");
			}
			LogicalByteArray wbuffer = TerminateDialogueMessage.marshal(m_ic.getDialogueId(), this.m_ic);

			//
			// used m_ic instead of getInputOutput, because getInputOutput
			// implicitly calls close at timeout, which will call
			// TerminateDialogue
			// which causes recursion.
			//
			// m_io.setTimeout(m_ic.getT4props().getCloseConnectionTimeout());
			m_io.setTimeout(getT4props().getLoginTimeout());

			LogicalByteArray rbuffer = getReadBuffer(TRANSPORT.SRVR_API_SQLDISCONNECT, wbuffer);

			//
			// Process output parameters
			//
			TerminateDialogueReply tdr1 = new TerminateDialogueReply(rbuffer, m_ic.getNCSAddress().getIPorName(), m_ic);

			//
			// Send a close message and close the port if we don't have an
			// error.
			// If there is an error, it's up to the calling routine to decide
			// what to do.
			//
			if (tdr1.m_p1.exception_nr == TRANSPORT.CEE_SUCCESS) {
				m_io.closeIO();
			}

			closeTimers();
            if (getT4props().isLogEnable(Level.FINEST)) {
                T4LoggingUtilities.log(getT4props(), Level.FINEST, "LEAVE");
            }
            if (LOG.isTraceEnabled()) {
                LOG.trace("LEAVE");
            }
			return tdr1;
		} // end try
		catch (SQLException se) {
			throw se;
		} catch (CharacterCodingException e) {
			SQLException se = TrafT4Messages.createSQLException(getT4props(), 
					"translation_of_parameter_failed", "TerminateDialogMessage", e.getMessage());
			se.initCause(e);
			throw se;
		} catch (UnsupportedCharsetException e) {
			SQLException se = TrafT4Messages.createSQLException(getT4props(), "unsupported_encoding", e
					.getCharsetName());
			se.initCause(e);
			throw se;
		} catch (Exception e) {
			SQLException se = TrafT4Messages.createSQLException(getT4props(), 
					"terminate_dialogue_message_error", e.getMessage());

			se.initCause(e);
			throw se;
		} // end catch
	} // end TerminateDialogue

	/**
	 * This method will send a set connection option command to the server.
	 * 
	 * @param connetionOption
	 *            The connection option to be set
	 * @param optionValueNum
	 *            The number value of the option
	 * @param optionValueStr
	 *            The string value of the option
	 * 
	 * @retrun a SetConnectionOptionReply class representing the reply from the
	 *         ODBC server is returned
	 * 
	 * @exception A
	 *                SQLException is thrown
	 */
	SetConnectionOptionReply SetConnectionOption(short connectionOption, int optionValueNum, String optionValueStr)
			throws SQLException {

		if (optionValueStr == null) {
			throwInternalException();

		}
		try {

			LogicalByteArray wbuffer = SetConnectionOptionMessage.marshal(m_ic.getDialogueId(), connectionOption,
					optionValueNum, optionValueStr, this.m_ic);

			getInputOutput().setTimeout(m_ic.getQueryTimeout());

			LogicalByteArray rbuffer = getReadBuffer(TRANSPORT.SRVR_API_SQLSETCONNECTATTR, wbuffer);

			SetConnectionOptionReply scor = new SetConnectionOptionReply(rbuffer, m_ic.getNCSAddress().getIPorName(), m_ic);

			return scor;
		} // end try
		catch (SQLException se) {
			throw se;
		} catch (CharacterCodingException e) {
			SQLException se = TrafT4Messages.createSQLException(getT4props(), 
					"translation_of_parameter_failed", "SetConnectionOptionReply", e.getMessage());
			se.initCause(e);
			throw se;
		} catch (UnsupportedCharsetException e) {
			SQLException se = TrafT4Messages.createSQLException(getT4props(), "unsupported_encoding", e
					.getCharsetName());
			se.initCause(e);
			throw se;
		} catch (Exception e) {
			SQLException se = TrafT4Messages.createSQLException(getT4props(), 
					"set_connection_option_message_error", e.getMessage());

			se.initCause(e);
			throw se;
		} // end catch
	} // end SetConnectionOption

	/**
	 * This method will send an End Transaction command, which does not return
	 * any rowsets, to the ODBC server.
	 * 
	 * @param transactionOpt
	 *            A transaction opt
	 * 
	 * @retrun A EndTransactionReply class representing the reply from the ODBC
	 *         server is returned
	 * 
	 * @exception A
	 *                SQLException is thrown
	 */
	EndTransactionReply endTransaction(short transactionOpt) throws SQLException {

		try {
			LogicalByteArray wbuffer = EndTransactionMessage.marshal(m_ic.getDialogueId(), transactionOpt, this.m_ic);

			getInputOutput().setTimeout(m_ic.getQueryTimeout());

			long start = 0;
			if (this.m_ic.getT4props().isTraceTransTime() && !this.m_ic.getAutoCommit()) {
			    start = System.currentTimeMillis();
			    this.m_ic.getTrafT4Conn().checkTransStartTime(start);
			}
			LogicalByteArray rbuffer = getReadBuffer(TRANSPORT.SRVR_API_SQLENDTRAN, wbuffer);
			this.m_ic.getTrafT4Conn().addTransactionUsedTimeList("endTransaction", "endTransaction", start, TRANSPORT.TRACE_ENDTRANSACTION_TIME);
			EndTransactionReply cr = new EndTransactionReply(rbuffer, m_ic.getNCSAddress().getIPorName(), m_ic);
			return cr;
		} // end try
		catch (SQLException se) {
			throw se;
		} catch (CharacterCodingException e) {
			SQLException se = TrafT4Messages.createSQLException(getT4props(), 
					"translation_of_parameter_failed", "EndTransactionMessage", e.getMessage());
			se.initCause(e);
			throw se;
		} catch (UnsupportedCharsetException e) {
			SQLException se = TrafT4Messages.createSQLException(getT4props(), "unsupported_encoding", e
					.getCharsetName());
			se.initCause(e);
			throw se;
		} catch (Exception e) {
			SQLException se = TrafT4Messages.createSQLException(getT4props(), "end_transaction_message_error",
					e.getMessage());

			se.initCause(e);
			throw se;
		} // end catch

	} // end EndTransaction

     /**
      * This method will send an savepoint option
      *
      * @param SavepointOpt A savepoint opt
      * @param SavepointName A savepoint name
      *
      * @return A SavepointReply class representing the reply from the ODBC server is returned
      *
      * @exception A SQLException is thrown
      */
    SavepointReply savepointOpt(short savepointOpt, String savepointName) throws SQLException {
        try {
            LogicalByteArray wbuffer =
                SavepointMessage.marshal(m_ic.getDialogueId(), savepointOpt, savepointName, this.m_ic);
            getInputOutput().setTimeout(getT4props().getNetworkTimeout());
            LogicalByteArray rbuffer = getReadBuffer(TRANSPORT.SRVR_API_SAVEPOINT, wbuffer);
            SavepointReply cr = new SavepointReply(rbuffer, m_ic.getNCSAddress().getIPorName(), m_ic);
            return cr;
        } catch (CharacterCodingException e) {
            SQLException se = TrafT4Messages.createSQLException(getT4props(),
                    "translation_of_parameter_failed", "SavepointMessage", e.getMessage());
            se.initCause(e);
            throw se;
        } catch (UnsupportedCharsetException e) {
            SQLException se = TrafT4Messages.createSQLException(getT4props(),
                    "unsupported_encoding", e.getCharsetName());
            se.initCause(e);
            throw se;
        } catch (Exception e) {
            SQLException se = TrafT4Messages.createSQLException(getT4props(),
                    "savepoint_option_error", savepointName, savepointOpt, e.getMessage());
            se.initCause(e);
            throw se;
        }
    }

	/**
	 * This method will send an get SQL catalogs command to the ODBC server.
	 * 
	 * @param stmtLabel
	 *            a statement label for use by the ODBC server
	 * @param APIType
	 * @param catalogNm
	 * @param schemaNm
	 * @param tableNm
	 * @param tableTypeList
	 * @param columnNm
	 * @param columnType
	 * @param rowIdScope
	 * @param nullable
	 * @param uniqueness
	 * @param accuracy
	 * @param sqlType
	 * @param metadataId
	 * @param fkcatalogNm
	 * @param fkschemaNm
	 * @param fktableNm
	 * 
	 * @retrun a GetSQLCatalogsReply class representing the reply from the ODBC
	 *         server is returned
	 * 
	 * @exception A
	 *                SQLException is thrown
	 */
	GetSQLCatalogsReply GetSQLCatalogs(String stmtLabel, short APIType, String catalogNm, String schemaNm,
			String tableNm, String tableTypeList, String columnNm, int columnType, int rowIdScope, int nullable,
			int uniqueness, int accuracy, short sqlType, int metadataId, String fkcatalogNm, String fkschemaNm,
			String fktableNm) throws SQLException {

		if (stmtLabel == null) {
			throwInternalException();

		}
		try {
			LogicalByteArray wbuffer;

			wbuffer = GetSQLCatalogsMessage.marshal(m_ic.getDialogueId(), stmtLabel, APIType, catalogNm, schemaNm, tableNm,
					tableTypeList, columnNm, columnType, rowIdScope, nullable, uniqueness, accuracy, sqlType,
					metadataId, fkcatalogNm, fkschemaNm, fktableNm, m_ic);

			getInputOutput().setTimeout(m_ic.getQueryTimeout());

			LogicalByteArray rbuffer = getReadBuffer(TRANSPORT.SRVR_API_GETCATALOGS, wbuffer);

			//
			// Process output parameters
			//
			GetSQLCatalogsReply gscr = new GetSQLCatalogsReply(rbuffer, m_ic.getNCSAddress().getIPorName(), m_ic);

			return gscr;
		} // end try
		catch (SQLException se) {
			throw se;
		} catch (CharacterCodingException e) {
			SQLException se = TrafT4Messages.createSQLException(getT4props(), 
					"translation_of_parameter_failed", "GetSQLCatalogsMessage", e.getMessage());
			se.initCause(e);
			throw se;
		} catch (UnsupportedCharsetException e) {
			SQLException se = TrafT4Messages.createSQLException(getT4props(), "unsupported_encoding", e
					.getCharsetName());
			se.initCause(e);
			throw se;
		} catch (Exception e) {
			SQLException se = TrafT4Messages.createSQLException(getT4props(), 
					"get_sql_catalogs_message_error", e.getMessage());

			se.initCause(e);
			throw se;
		} // end catch

	} // end GetSQLCatalogs

}
