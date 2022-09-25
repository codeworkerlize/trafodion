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

import java.io.UnsupportedEncodingException;
import java.lang.ref.Reference;
import java.lang.ref.ReferenceQueue;
import java.net.ConnectException;
import java.net.InetAddress;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.UnsupportedCharsetException;
import java.sql.Connection;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Savepoint;
import java.util.Hashtable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.Map.Entry;
import java.util.logging.Level;
import org.slf4j.LoggerFactory;
import org.trafodion.jdbc.t4.utils.Base64;
import org.trafodion.jdbc.t4.utils.Utils;


class InterfaceConnection {
	static final int MODE_SQL = 0;
	static final int MODE_WMS = 1;
	static final int MODE_CMD = 2;
	
	static final short SQL_COMMIT = 0;
	static final short SQL_ROLLBACK = 1;
    static final short SQL_BEGIN_SAVEPOINT = 0;
    static final short SQL_ROLLBACK_SAVEPOINT = 1;
    static final short SQL_COMMIT_SAVEPOINT = 2;
    static final short SQL_RELEASE_SAVEPOINT = 3;
	private int activeTimeBeforeCancel = -1;
	private int txnIsolationLevel = Connection.TRANSACTION_READ_COMMITTED;
	private boolean autoCommit = true;
	private boolean isReadOnly = false;
	private boolean isClosed_;
    private boolean processing;
	private long txid;
	private Locale locale;
	private USER_DESC_def userDesc;
	private CONNECTION_CONTEXT_def inContext;
	OUT_CONNECTION_CONTEXT_def outContext;
	private boolean useArrayBinding_;
	private short transportBufferSize_;
	private NCSAddress ncsAddr;
	protected T4DcsConnection t4DcsConnection;
	private T4Connection t4connection_;
	private String m_ncsSrvrRef;
	private int dialogueId;
	private String m_sessionName;

	// character set information
	private int isoMapping_ = 15;
	private int termCharset_ = 0;
	private boolean enforceISO = false;
	private boolean byteSwap = false;
	private String _serverDataSource;
	private ConnectReply _connectReply;
	private boolean reconn;
	private int ddlTimeout;
	
	T4Properties t4props;
	SQLWarning sqlwarning_;

	HashMap encoders = new HashMap(11);
	HashMap decoders = new HashMap(11);

	// static fields from odbc_common.h and sql.h
	static final int SQL_TXN_READ_UNCOMMITTED = 1;
	static final int SQL_TXN_READ_COMMITTED = 2;
	static final int SQL_TXN_REPEATABLE_READ = 4;
	static final int SQL_TXN_SERIALIZABLE = 8;
	static final short SQL_ATTR_CURRENT_CATALOG = 109;
	static final short SQL_ATTR_ACCESS_MODE = 101;
	static final short SQL_ATTR_AUTOCOMMIT = 102;
	static final short SQL_TXN_ISOLATION = 108;
	static final short SET_SCHEMA = 1001; // this value is follow server side definition


	static final short SET_SESSION_DEBUG = 1090; // this value is follow server side definition

	static final short SET_CLIPVARCHAR = 1091; // this value is follow server side definition

	// spj proxy syntax support
	static final short SPJ_ENABLE_PROXY = 1040;

	static final int PASSWORD_SECURITY = 0x4000000; //(2^26)
	static final int ROWWISE_ROWSET = 0x8000000; // (2^27);
	static final int CHARSET = 0x10000000; // (2^28)
	static final int STREAMING_DELAYEDERROR_MODE = 0x20000000; // 2^29
	static final int PASSWORD_SECURITY_BASE64 = 0x40000000; // 2^30
	// Zbig added new attribute on 4/18/2005
	static final short JDBC_ATTR_CONN_IDLE_TIMEOUT = 3000;
	static final short RESET_IDLE_TIMER = 1070;

	// for handling WeakReferences
	static ReferenceQueue refQ_ = new ReferenceQueue();
	static Hashtable refTosrvrCtxHandle_ = new Hashtable();

	//3196 - NDCS transaction for SPJ
	static final short SQL_ATTR_JOIN_UDR_TRANSACTION = 1041;
	static final short SQL_ATTR_SUSPEND_UDR_TRANSACTION = 1042;
	long transId_ = 0;
	boolean suspendRequest_ = false; 

	private boolean _ignoreCancel;
	
	private AtomicLong  _seqNum = new AtomicLong(0);
	long currentTime;
	
	private TrafT4Connection conn;
	private String remoteProcess;

    private String clientSocketAddress;
    private final static org.slf4j.Logger LOG = LoggerFactory.getLogger(InterfaceConnection.class);
    private int reconnNum = 1;
    private ArrayList<String> addresses;

	private SQLException connException;
	private boolean success;

    int getActiveTimeBeforeCancel() { return activeTimeBeforeCancel; }
	InterfaceConnection(TrafT4Connection conn, T4Properties t4props) throws SQLException {
        if (t4props.isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(t4props, Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        this.conn = conn;
        this.t4props = t4props;
        remoteProcess = "";
        // close any weak connections that need to be closed.
        gcConnections();

        if (t4props.getSQLException() != null) {
            throw TrafT4Messages.createSQLException(getT4props(), "invalid_property", t4props
                .getSQLException());
        }
        userDesc = getUserDescription(t4props.getUser());

		m_sessionName = getT4props().getSessionName();

		if (m_sessionName != null && m_sessionName.length() > 0) {
			if (m_sessionName.length() > 24)
				m_sessionName = m_sessionName.substring(0, 24);

            if (!m_sessionName.matches("\\w+"))
                throw TrafT4Messages.createSQLException(getT4props(), "Invalid sessionName.  Session names can only contain alphnumeric characters.");
		}

		locale = t4props.getLocale();
		txid = 0;
		isClosed_ = false;
		useArrayBinding_ = t4props.getUseArrayBinding();
		// transportBufferSize_ = t4props.getTransportBufferSize();
		transportBufferSize_ = 32000;

		// Connection context details
		inContext = getInContext(t4props);
		m_ncsSrvrRef = t4props.getUrl();
		_ignoreCancel = t4props.getIgnoreCancel();

		sqlwarning_ = null;

		// if clientCharset is not set, use column definition
        this.termCharset_ = t4props.getClientCharset() == null ? termCharset_
                : InterfaceUtilities.getCharsetValue(t4props.getClientCharset());

        this.reconn = false;
        addresses = conn.createAddressList(t4props);
	}

	protected T4Properties getT4props() {
        return t4props;
    }

    protected TrafT4Connection getTrafT4Conn() {
        return this.conn;
    }

	protected AtomicLong getSequenceNumber() {
		if(_seqNum.incrementAndGet() < 0) {
			_seqNum.set(1);
		}
		Long seqTmp = _seqNum.get();
        _seqNum.set(seqTmp);
		
		return _seqNum;
	}

	public void setT4DcsConnection(T4DcsConnection t4DcsConnection) {
		this.t4DcsConnection = t4DcsConnection;
	}

	protected void setSeqNum(AtomicLong seqNum) {
        this._seqNum = seqNum;
    }
	
	protected String getRemoteProcess() {
		return remoteProcess;
	}

	protected boolean isClosed() {
		return this.isClosed_;
	}

	String getRoleName() {
		return outContext._roleName;
	}

	boolean getIgnoreCancel() {
		return this._ignoreCancel;
	}

	CONNECTION_CONTEXT_def getInContext() {
		return inContext;
	}

	private CONNECTION_CONTEXT_def getInContext(T4Properties t4props) {
		inContext = new CONNECTION_CONTEXT_def();
		inContext.catalog = t4props.getCatalog();
        inContext.setSchema(t4props.getSchema());
		inContext.datasource = t4props.getServerDataSource();
		inContext.userRole = t4props.getRoleName();
		inContext.tenantName = t4props.getTenantName();
		inContext.cpuToUse = t4props.getCpuToUse();
		inContext.cpuToUseEnd = -1; // for future use by DBTransporter

		inContext.accessMode = (short) (isReadOnly ? 1 : 0);
		inContext.autoCommit = (short) (autoCommit ? 1 : 0);

		inContext.queryTimeoutSec = t4props.getQueryTimeout();
		inContext.idleTimeoutSec = (short) t4props.getConnectionTimeout();
        inContext.clipVarchar = (int) t4props.getClipVarchar();
        inContext.sessionDebug = (short) t4props.getSessionDebug();
        inContext.mds = (short) t4props.getMds();
		inContext.loginTimeoutSec = (short) t4props.getLoginTimeout();
                inContext.txnIsolationLevel = t4props.getTxnIsolationLevel();
		inContext.rowSetSize = t4props.getFetchBufferSize();
		inContext.diagnosticFlag = 0;
		inContext.processId = (int) System.currentTimeMillis() & 0xFFF;
		ddlTimeout = t4props.getDDLTimeout();
		try {
			inContext.computerName = InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException uex) {
			inContext.computerName = "Unknown Client Host";
		}
		inContext.windowText = t4props.getApplicationName();

		inContext.ctxDataLang = 15;
		inContext.ctxErrorLang = 15;

		inContext.ctxACP = 1252;
		inContext.ctxCtrlInferNXHAR = -1;
		inContext.clientVersionList.list = getVersion(inContext.processId);
		return inContext;
	}

	private VERSION_def[] getVersion(int pid) {
		short majorVersion = 13;
		short minorVersion = 0;
		int buildId = 0;

        this.setMajorVersion(majorVersion);
        this.setMinorVersion(minorVersion);

		VERSION_def version[] = new VERSION_def[2];

		// Entry [0] is the Driver Version information
		version[0] = new VERSION_def();
		version[0].componentId = 20;
		version[0].majorVersion = majorVersion;
		version[0].minorVersion = minorVersion;
		version[0].buildId = buildId | ROWWISE_ROWSET | CHARSET | PASSWORD_SECURITY | PASSWORD_SECURITY_BASE64;
		
		if (this.getT4props().getDelayedErrorMode())
	    {
	      version[0].buildId |= STREAMING_DELAYEDERROR_MODE;
	    }

		// Entry [1] is the Application Version information
		version[1] = new VERSION_def();
		version[1].componentId = 8;
		version[1].majorVersion = 3;
		version[1].minorVersion = 0;
		version[1].buildId = 0;

		return version;
	}

	USER_DESC_def getUserDescription() {
		return userDesc;
	}

	private void setISOMapping(int isoMapping) {
//		if (InterfaceUtilities.getCharsetName(isoMapping) == InterfaceUtilities.SQLCHARSET_UNKNOWN)
//			isoMapping = InterfaceUtilities.getCharsetValue("ISO8859_1");

		isoMapping_ = InterfaceUtilities.getCharsetValue("UTF-8");;
	}

	String getServerDataSource() {
		return this._serverDataSource;
	}

	boolean getEnforceISO() {
		return enforceISO;
	}

	int getISOMapping() {
		return isoMapping_;
	}

	protected String getSessionName() {
		return m_sessionName;
	}

	private void setTerminalCharset(int termCharset) {
//		if (InterfaceUtilities.getCharsetName(termCharset) == InterfaceUtilities.SQLCHARSET_UNKNOWN)
//			termCharset = InterfaceUtilities.getCharsetValue("ISO8859_1");

		termCharset_ = InterfaceUtilities.getCharsetValue("UTF-8");;
	}

	int getTerminalCharset() {
		return termCharset_;
	}

	private USER_DESC_def getUserDescription(String user) throws SQLException {
		userDesc = new USER_DESC_def();
		userDesc.userDescType = (this.getT4props().getSessionToken()) ? TRANSPORT.PASSWORD_ENCRYPTED_USER_TYPE
				: TRANSPORT.UNAUTHENTICATED_USER_TYPE;
		userDesc.userName = (user.length() > 128) ? user.substring(0, 128) : user;
		userDesc.domainName = "";

		userDesc.userSid = null;
		userDesc.password = null; //we no longer want to send the password to the MXOAS

		return userDesc;
	}

	private void oldEncryptPassword() throws SQLException {
		String pwd = this.getT4props().getPassword();
		
		if (pwd.length() > 386)
			pwd = pwd.substring(0, 386);

		byte [] authentication;
		try {
			authentication = pwd.getBytes("US-ASCII");
		} catch (UnsupportedEncodingException uex) {
			throw TrafT4Messages.createSQLException(getT4props(), uex.getMessage());
		}

		if (authentication.length > 0) {
			Utility.Encryption(authentication, authentication, authentication.length);
		}
		
		userDesc.password = authentication;
	}

    private void base64EncryptPassword() throws SQLException {
        String pwd = this.getT4props().getPassword();

        if (pwd.length() > 386)
            pwd = pwd.substring(0, 386);

        try {
            userDesc.password = Base64.getEncoder().encode(pwd.getBytes("US-ASCII"));
        } catch (UnsupportedEncodingException uex) {
            throw TrafT4Messages.createSQLException(getT4props(), uex.getMessage());
        }
    }

    T4Connection getT4Connection() {
        return t4connection_;
    }

	int getDialogueId() {
		return dialogueId;
	}

	int getQueryTimeout() {
		return inContext.queryTimeoutSec;
	}

	int getDDLTimeout() { return this.ddlTimeout; }

	int getLoginTimeout() {
		return inContext.loginTimeoutSec;
	}

	int getConnectionTimeout() {
		return inContext.idleTimeoutSec;
	}
    int getClipVarchar() {
        return inContext.clipVarchar;
	}
	


    short getMds() {
        return inContext.mds;
    }

	short getSessionDebug() {
		return inContext.sessionDebug;
	}

	String getCatalog() {
		if (outContext != null) {
			return outContext.catalog;
		} else {
			return inContext.catalog;
		}
	}

	boolean getDateConversion() {
		return ((outContext.versionList.list[0].buildId & 512) > 0);
	}

	int getServerMajorVersion() {
		return outContext.versionList.list[1].majorVersion;
	}

	int getServerMinorVersion() {
		return outContext.versionList.list[1].minorVersion;
	}

	String getUid() {
		return userDesc.userName;
	}

    String getSchema() {
        if (outContext != null) {
            return outContext.getSchema();
        } else {
            return inContext.getSchema();
        }
    }

    void setSchemaDirect(String schema) {
        outContext.setSchema(schema);
    }
    void setSchema(TrafT4Connection conn, String schema) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", schema);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", schema);
        }
        if (schema == null || schema.length() == 0) {
            return;
        }
        setConnectionAttr(conn, SET_SCHEMA, 0, schema);
        setSchemaDirect(schema);

        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "LEAVE", schema);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("LEAVE. {}", schema);
        }
    }

	void setLocale(Locale locale) {
		this.locale = locale;
	}

	Locale getLocale() {
		return locale;
	}

	boolean getByteSwap() {
		return this.byteSwap;
	}

	NCSAddress getNCSAddress() {
		return ncsAddr;
	}
	
	void commit() throws SQLException {
		endTransaction(SQL_COMMIT);
	}

	void rollback() throws SQLException {
		endTransaction(SQL_ROLLBACK);
	}

    void rollbackSavepoint(Savepoint savepoint) throws SQLException {
        savepointOpt(SQL_ROLLBACK_SAVEPOINT, savepoint.getSavepointName());
    }
    void setSavepoint(Savepoint savepoint) throws SQLException {
        savepointOpt(SQL_BEGIN_SAVEPOINT, savepoint.getSavepointName());
    }
    void commitSavepoint(Savepoint savepoint) throws SQLException {
        savepointOpt(SQL_COMMIT_SAVEPOINT, savepoint.getSavepointName());
    }
    void releaseSavepoint(Savepoint savepoint) throws SQLException {
        savepointOpt(SQL_RELEASE_SAVEPOINT, savepoint.getSavepointName());
    }

	void cancel() throws SQLException {
		if(!this._ignoreCancel) {
			String srvrObjRef = "" + ncsAddr.getPort();
			// String srvrObjRef = t4props_.getServerID();
			int srvrType = 2; // AS server
			CancelReply cr_ = null;

			if (t4props.t4Logger_.isLoggable(Level.FINEST) == true) {
				Object p[] = T4LoggingUtilities.makeParams(t4props);
				String temp = "cancel request received for " + srvrObjRef;
				t4props.t4Logger_.logp(Level.FINEST, "InterfaceConnection", "connect", temp, p);
			}

			//
			// Send the cancel to the ODBC association server.
			//
			String errorText = null;
			int tryNum = 0;
			String errorMsg = null;
			String errorMsg_detail = null;
			long currentTime = (new java.util.Date()).getTime();
			long endTime;

			if (inContext.loginTimeoutSec > 0) {
				endTime = currentTime + inContext.loginTimeoutSec * 1000;
			} else {

				// less than or equal to 0 implies infinit time out
				endTime = Long.MAX_VALUE;

				//
				// Keep trying to contact the Association Server until we run out of
				// time, or make a connection or we exceed the retry count.
				//
			}
			cr_ = t4DcsConnection.cancel(srvrType,  srvrObjRef, 0);


			switch (cr_.m_p1_exception.exception_nr) {
				case TRANSPORT.CEE_SUCCESS:
					if (t4props.t4Logger_.isLoggable(Level.FINEST) == true) {
						Object p[] = T4LoggingUtilities.makeParams(t4props);
						String temp = "Cancel successful";
						t4props.t4Logger_.logp(Level.FINEST, "InterfaceConnection", "connect", temp, p);
					}
					break;
				default:

					//
					// Some unknown error
					//
					if (cr_.m_p1_exception.clientErrorText != null) {
						errorText = "Client Error text = " + cr_.m_p1_exception.clientErrorText;
					}
					errorText = errorText + "  :Exception = " + cr_.m_p1_exception.exception_nr;
					errorText = errorText + "  :" + "Exception detail = " + cr_.m_p1_exception.exception_detail;
					errorText = errorText + "  :" + "Error code = " + cr_.m_p1_exception.errorCode;

					if (t4props.t4Logger_.isLoggable(Level.FINEST) == true) {
						Object p[] = T4LoggingUtilities.makeParams(t4props);
						String temp = errorText;
						t4props.t4Logger_.logp(Level.FINEST, "InterfaceConnection", "cancel", temp, p);
					}
					throw TrafT4Messages.createSQLException(t4props, "as_cancel_message_error", errorText);
			} // end switch

			currentTime = (new java.util.Date()).getTime();
		}
	}

	void cancel(long startTime) throws SQLException 
	{
		long currentTime;
		if (startTime != -1) {
			if (activeTimeBeforeCancel != -1) {
				currentTime = System.currentTimeMillis();
				if ((activeTimeBeforeCancel * 1000) < (currentTime - startTime))
					return; 
			}
		}
		String srvrObjRef = "" + ncsAddr.getPort();
		// String srvrObjRef = t4props_.getServerID();
		int srvrType = 2; // AS server
        this.doCancel(srvrType, srvrObjRef, 0);
	}

    void cancel(String qid) throws SQLException {
        String srvrObjRef = "" + ncsAddr.getPort() + ":"+qid;
        if (cancelFlag_) return;
        // String srvrObjRef = t4props_.getServerID();
        int srvrType = 2; // AS server
	    this.doCancel(srvrType, srvrObjRef, 0);
    }
	
	private boolean initDiag(boolean setTimestamp, boolean downloadCert) throws SQLException {
		short retryCount = 3;
		InitializeDialogueReply idr = null;
		long endTime = (inContext.loginTimeoutSec > 0) ? currentTime + inContext.loginTimeoutSec * 1000 : Long.MAX_VALUE;
		int tryNum = 0;
		boolean done = false;

		boolean socketException = false;
		SQLException seSave = null;

		do {
            if (getT4props().isLogEnable(Level.FINER)) {
                String temp = "Attempting initDiag.  Try " + (tryNum + 1) + " of " + retryCount;
                T4LoggingUtilities.log(getT4props(), Level.FINER, temp);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Attempting initDiag.  Try {} of {}", (tryNum + 1), retryCount);
            }

			socketException = false;
			try {
				t4connection_ = new T4Connection(this);
				long startInitialTime = System.currentTimeMillis();
				idr = t4connection_.InitializeDialogue(setTimestamp, downloadCert);
				long endInitialTime = System.currentTimeMillis();
				if (endInitialTime - startInitialTime > getT4props().getMxoConnectTimeThreshold()) {
					if (getT4props().isLogEnable(Level.WARNING)) {
						String temp = "Spent " + (endInitialTime - startInitialTime)
								+ " ms to initial dialogue with mxosrvr, which is over threshold "
								+ getT4props().getMxoConnectTimeThreshold() + " ms.";
						T4LoggingUtilities.log(getT4props(), Level.WARNING, temp);
					}
					if (LOG.isWarnEnabled()) {
						LOG.warn("Spent {} ms to initial dialogue with mxosrvr, which is over threshold {} ms.",
								endInitialTime - startInitialTime,
								getT4props().getMxoConnectTimeThreshold());
					}
				}

			} catch (SQLException se) {
				//
				// We will retry socket exceptions, but will fail on all other
				// exceptions.
				//
				int sc = se.getErrorCode();
				int s1 = TrafT4Messages.getErrorCode(getT4props(), "socket_open_error");
				int s2 = TrafT4Messages.getErrorCode(getT4props(), "socket_write_error");
				int s3 = TrafT4Messages.getErrorCode(getT4props(), "socket_read_error");
                // TODO
                // it can't sure which exception should do retry, and which one should throw directly,
                // so we reserve catch here, to fill which exception should be catch.
                // address_lookup_error
                int problem_with_server_read = TrafT4Messages.getErrorCode(getT4props(), "problem_with_server_read");
                int session_close_error = TrafT4Messages.getErrorCode(getT4props(), "session_close_error");

				if (sc == s1 || sc == s2 || sc == s3 || sc == problem_with_server_read || sc == session_close_error) {
		            if (getT4props().isLogEnable(Level.FINER)) {
                        String temp = "A socket exception occurred: " + se.getMessage();
		                T4LoggingUtilities.log(getT4props(), Level.FINER, temp);
		            }
		            if (LOG.isDebugEnabled()) {
		                LOG.debug("A socket exception occurred: {}", se.getMessage());
		            }

					socketException = true;
					seSave = se;
				} else {
					if (getT4props().isLogEnable(Level.FINER)) {
                        String temp = "A non-socket fatal exception occurred: " + se.getMessage();
                        T4LoggingUtilities.log(getT4props(), Level.FINER, temp);
                    }
					if (LOG.isDebugEnabled()) {
					    LOG.debug("A non-socket fatal exception occurred: {}", se.getMessage());
					}

					try {
						t4connection_.getInputOutput().closeIO();
					} catch (Exception e) {
						// ignore error
					}
					
					int mxosrvrExit = TrafT4Messages.getErrorCode(getT4props(), "socket_write_error_2");
					if(se.getErrorCode() == mxosrvrExit) {
					    return false;
					}

					throw se;
				}
			}


			if (socketException == false) {
				if (idr.exception_nr == TRANSPORT.CEE_SUCCESS) {
					done = true;
                    if (idr.exception_detail == odbc_SQLSvc_InitializeDialogue_exc_.SQL_PASSWORD_EXPIRING){
                        TrafT4Messages.setSQLWarning(this.getT4props(), this.conn, "*** WARN[8857] The validity of the password is less than 7 days");
                    }
                    else if(idr.exception_detail == odbc_SQLSvc_InitializeDialogue_exc_.SQL_PASSWORD_GRACEPERIOD){
                        TrafT4Messages.setSQLWarning(this.getT4props(), this.conn, "*** WARN[8837] User password has expired,using grace login");
                        TrafT4Messages.setSQLWarning(this.getT4props(), this.conn, "*** WARN[8837] Please change your password as soon as possible");
                    }
                    if (getT4props().isLogEnable(Level.FINER)) {
                        String temp = "initDiag Successful.";
                        T4LoggingUtilities.log(getT4props(), Level.FINER, temp);
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("initDiag Successful.");
                    }
				} else if (idr.exception_nr == odbc_SQLSvc_InitializeDialogue_exc_.odbc_SQLSvc_InitializeDialogue_SQLError_exn_ || 
						idr.exception_nr == odbc_SQLSvc_InitializeDialogue_exc_.odbc_SQLSvc_InitializeDialogue_InvalidUser_exn_) {
                    if (getT4props().isLogEnable(Level.FINER)) {
                        String temp = "A SQL Warning or Error occurred during initDiag: " + idr.SQLError;
                        T4LoggingUtilities.log(getT4props(), Level.FINER, temp);
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("A SQL Warning or Error occurred during initDiag: {}",
                                idr.SQLError);
                    }

					int ex_nr = idr.exception_nr;
					int ex_nr_d = idr.exception_detail;

					if (ex_nr_d == odbc_SQLSvc_InitializeDialogue_exc_.SQL_PASSWORD_EXPIRING ||
							ex_nr_d == odbc_SQLSvc_InitializeDialogue_exc_.SQL_PASSWORD_GRACEPERIOD) {
						TrafT4Messages.setSQLWarning(this.getT4props(), this.conn, idr.SQLError);
						done = true;
					} else {
						TrafT4Messages.throwSQLException(getT4props(), idr.SQLError);
					}
				}
			}

			currentTime = System.currentTimeMillis();
			tryNum = tryNum + 1;
		} while (done == false && endTime > currentTime && tryNum < retryCount);

        if (done == false) {
            SQLException se1;

            if (socketException == true) {
                throw seSave;
            }

            if (currentTime >= endTime) {
                se1 = TrafT4Messages.createSQLException(getT4props(), "ids_s1_t00", getTargetServer(), getLoginTimeout());
            } else if (tryNum >= retryCount) {
                se1 = TrafT4Messages.createSQLException(getT4props(), "as_connect_message_error", "exceeded retry count");
            } else {
                se1 = TrafT4Messages.createSQLException(getT4props(), "as_connect_message_error");
            }
            throw se1;
        }

		//
		// Set the outcontext value returned by the ODBC MX server in the
		// serverContext
		//
		outContext = idr.outContext;
		enforceISO = outContext._enforceISO;
		this._ignoreCancel = outContext._ignoreCancel;

		getT4props().setNcsMajorVersion(idr.outContext.versionList.list[0].majorVersion);
		getT4props().setNcsMinorVersion(idr.outContext.versionList.list[0].minorVersion);
		getT4props().setSqlmxMajorVersion(idr.outContext.versionList.list[1].majorVersion);
		getT4props().setSqlmxMinorVersion(idr.outContext.versionList.list[1].minorVersion);

        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "LEAVE");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("LEAVE");
        }
		return true;
	}
	
	private void encryptPassword(SecPwd security) throws SecurityException, SQLException {
		byte [] pwBytes;
		byte [] roleBytes;
		
		String roleName = getT4props().getRoleName();
		
		try {
			pwBytes = getT4props().getPassword().getBytes("US-ASCII");
			roleBytes = (roleName != null && roleName.length() > 0)?roleName.getBytes("US-ASCII"):null;
		}
		catch (UnsupportedEncodingException uex) {
			//ERROR MESSAGE
			throw new SQLException("failed to find encoding");
		}
		
		userDesc.password = new byte[security.getPwdEBufferLen()];
	
		security.encryptPwd(pwBytes, roleBytes, userDesc.password);
	}
	
	private byte [] createProcInfo(int pid, int nid, byte [] timestamp) throws SQLException {
		byte [] procInfo;

		procInfo = new byte[16];
		
		ByteBuffer bb = ByteBuffer.allocate(16);
		bb.order(ByteOrder.LITTLE_ENDIAN);
		bb.putInt(pid);
		bb.putInt(nid);
		bb.put(timestamp);
		bb.rewind();
		bb.get(procInfo, 0, 16);
		
		return procInfo;
	}

	private boolean secureLogin(ConnectReply cr) throws SQLException {
	    SecPwd security = null;
	    try {
			byte [] procInfo = createProcInfo(cr.processId, cr.serverNode, cr.timestamp);
			boolean tokenAuth = this.getT4props().getSPJEnv() && this.getT4props().getTokenAuth();
			//this.set_connectReply(cr);

			security = SecPwd.getInstance(
					this.conn,
					this.getT4props().getCertificateDir(),
					this.getT4props().getCertificateFile(),
					cr.clusterName, 
					tokenAuth,
					procInfo
					);
		} catch(SecurityException se) {
			CleanupServer(); //MXOSRVR is expecting InitDiag, clean it up since we failed
			throw se;
		}

		try {
			security.openCertificate();
			encryptPassword(security);
		}catch(SecurityException se) {	
			if(se.getErrorCode() != 29713 && se.getErrorCode() != 29721) {
				throw se; //we have a fatal error
			}
				
			DownloadCertificate(security); //otherwise, download and continue
		}
		boolean relVal = false;
		try {
			inContext.connectOptions = new String(security.getCertExpDate());
			relVal = initDiag(true,false);
		}catch(SQLException e) {
			if(outContext != null && outContext.certificate != null) { //we got a certificate back, switch to it, continue
				security.switchCertificate(outContext.certificate);
			}
			else { 
				throw e;
			}

            // when use ion in client tools like trafci, it will have outContext in memory,
            // so it will not throw exception such as user invalid, then do a initDiag again,
            // but actually , we need throw exception directly when met error such as user invalid
            if (e.getErrorCode() == TrafT4Handle.SQL_ERR_CLI_AUTH) {
                throw e;
            }
			
			inContext.connectOptions = new String(security.getCertExpDate());
			encryptPassword(security);  //re-encrypt
			relVal = this.initDiag(true,false); //re-initdiag
		}
		return relVal;
	}

	private void CleanupServer() throws SQLException {
		this.userDesc.userName = null;
		this.userDesc.password = null;
		
		try {
			initDiag(false,false); //send dummy init diag to clean up server
		}catch(SQLException e) {
			
		}

	}
	
	private void DownloadCertificate(SecPwd security) throws SQLException {
		//attempt download
		this.userDesc.userName = null;
		this.userDesc.password = null;
		inContext.connectOptions = null;
		
		try {
			initDiag(true,true);
		}catch(SQLException e) {
			if(outContext == null || outContext.certificate == null) {
				SQLException he = TrafT4Messages.createSQLException(getT4props(), "certificate_download_error", e.getMessage());
				he.setNextException(e);
				throw he;
			}
		}
		
		this.userDesc.userName = this.getT4props().getUser();
		
		try {
			security.switchCertificate(outContext.certificate);
			encryptPassword(security);
		}catch(SecurityException se1) {
			throw se1;
		}
	}

	protected boolean connect(String addr) throws SQLException {
		connException = null;
		success = false;
		doConnect(addr);
		if (!success) {
			throw connException;
		}
		if (getT4props().isLogEnable(Level.FINER)) {
			T4LoggingUtilities.log(getT4props(), Level.FINER, "ENTRY", addr);
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("ENTRY. Active Dcsmaster addresses is <{}>", addr);
		}
		return success;
	}

	protected void connect() throws SQLException {

		if (getT4props().isLogEnable(Level.FINER)) {
			T4LoggingUtilities.log(getT4props(), Level.FINER, "ENTRY");
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("ENTRY. Do Connect...");
		}
		connException = null;
		success = false;
		for (int i = 0; i < addresses.size(); i++) {
			try {
				if (doConnect(addresses.get(i))) {
					break;
				}
			} catch (SQLException e) {
				if(LOG.isWarnEnabled()){
					LOG.warn("connect failed with address {}, retrycount is {}", addresses.get(i), i+1);
				}
			}

		}
		if (!success) {
			throw connException;
		}
	}

	private boolean doConnect(String addre) throws SQLException {
		this.conn.clearWarnings();
		t4props.setUrl(addre);
		if (getT4props().isLogEnable(Level.FINER)) {
			String temp = "Association Server URL: " + m_ncsSrvrRef;
			T4LoggingUtilities.log(getT4props(), Level.FINER, temp);
		}
		if (LOG.isDebugEnabled()) {
			LOG.debug("Association Server URL: {}", m_ncsSrvrRef);
		}

		short retryCount = 60;
		int srvrType = 2; // AS server
		boolean reallocate = false;
		int reallocateNum = 1;

		try {
			do {
				if (reallocate && getT4props().isLogEnable(Level.FINER)) {
					String temp =
							"do reallocate mxosrvr, previous failure. Try" + reallocateNum + " of "
									+ retryCount;
					T4LoggingUtilities.log(getT4props(), Level.FINER, temp);
				}
				if (LOG.isDebugEnabled()) {
					LOG.debug("do reallocate mxosrvr, previous failure. Try {} of {}",
							reallocateNum,
							retryCount);
				}
				if (reallocate) {
					getUserDescription(getT4props().getUser());
					getInContext(getT4props());
					this.byteSwap = false;
				}
				//
				// Connect to the association server.
				//
				ConnectReply cr = null;
				String errorText = null;
				boolean done = false;
				int tryNum = 2;
				String errorMsg = null;
				String errorMsg_detail = null;
				currentTime = System.currentTimeMillis();
				long endTime =
						(inContext.loginTimeoutSec > 0) ? currentTime
								+ inContext.loginTimeoutSec * 1000
								: Long.MAX_VALUE;

				do {
					this.conn.checkLoginTimeout(getT4props());
					if (getT4props().isLogEnable(Level.FINER)) {
						String temp =
								"Attempting getObjRef. Try " + (tryNum + 1) + " of " + retryCount;
						T4LoggingUtilities.log(getT4props(), Level.FINER, temp);
					}
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("Attempting getObjRef. Try {} of {}", (tryNum + 1), retryCount);
                    }

                    t4DcsConnection = new T4DcsConnection(this);
                    long startConnectTime = System.currentTimeMillis();
                    try {
                        cr = t4DcsConnection.getConnection(srvrType, retryCount);
                        if (cr.securityEnabledBase64) {
                            inContext.clientVersionList.list[0].buildId |= PASSWORD_SECURITY_BASE64;
                        }
                    } catch (TrafT4Exception e) {
                        throw e;
                    } catch (SQLException  e) {
                        if (e.getErrorCode() == -29228) {
                            if (LOG.isWarnEnabled()) {
                                LOG.warn("connect to dcs failed over 10s, do retry ", e);
                            }
                            throw e;
                        }

                        done = false;
                        ++tryNum;
                        errorMsg = e.getMessage();
                        errorMsg_detail = errorMsg;
                        continue;
                    }
					long endConnectTime = System.currentTimeMillis();
					if (endConnectTime - startConnectTime > getT4props()
							.getDcsConnectTimeThreshold()) {
						if (getT4props().isLogEnable(Level.WARNING)) {
							String temp = "Spent " + (endConnectTime - startConnectTime)
									+ " ms to get connect reply from DCS "
									+ addre + ", which is over connect threshold "
									+ getT4props().getDcsConnectTimeThreshold() + " ms.";
							T4LoggingUtilities.log(getT4props(), Level.WARNING, temp);
						}
						if (LOG.isWarnEnabled()) {
							LOG.warn(
									"Spent {} ms to get connect reply from DCS {}, which is over connect threshold {} ms.",
									endConnectTime - startConnectTime,
									addre,
									getT4props().getDcsConnectTimeThreshold());
						}
					}
					if (LOG.isDebugEnabled()) {
						LOG.debug("Attempting getObjRef. Try {} of {}", (tryNum + 1),
								retryCount);
					}

					switch (cr.m_p1_exception.exception_nr) {
						case TRANSPORT.CEE_SUCCESS:
							done = true;
							if (getT4props().isLogEnable(Level.FINER)) {
								String temp =
										"getObjRef Successful. Server URL: " + cr.m_p2_srvrObjRef;
								T4LoggingUtilities.log(getT4props(), Level.FINER, temp);
							}
							if (LOG.isDebugEnabled()) {
								LOG.debug("getObjRef Successful. Server URL: {}",
										cr.m_p2_srvrObjRef);
							}
							if (!cr.m_p4_dataSource
									.equals(getT4props().getServerDataSource())) {
								Object[] messageArguments = new Object[1];
								messageArguments[0] = cr.m_p4_dataSource;
								sqlwarning_ = TrafT4Messages.createSQLWarning(getT4props(),
										"connected_to_Default_DS", messageArguments);
							}
							T4Properties.cachedUrl = getT4props().getUrl();
							break;
						case odbc_Dcs_GetObjRefHdl_exc_.odbc_Dcs_GetObjRefHdl_ASTryAgain_exn_:
							done = false;
							tryNum = tryNum + 1;
							errorMsg = "as_connect_message_error";
							errorMsg_detail = "try again request";
							if (tryNum < retryCount) {
								try {
									Thread.sleep(5000);
								} catch (Exception e) {
								}
							}
							break;
						case odbc_Dcs_GetObjRefHdl_exc_.odbc_Dcs_GetObjRefHdl_ASNotAvailable_exn_:
							done = false;
							tryNum = tryNum + 1;
							errorMsg = "as_connect_message_error";
							errorMsg_detail = "association server not available";
							break;
						case odbc_Dcs_GetObjRefHdl_exc_.odbc_Dcs_GetObjRefHdl_DSNotAvailable_exn_:
							done = false;
							tryNum = tryNum + 1;
							errorMsg = "as_connect_message_error";
							errorMsg_detail = "data source not available";
							break;
						case odbc_Dcs_GetObjRefHdl_exc_.odbc_Dcs_GetObjRefHdl_PortNotAvailable_exn_:
							done = false;
							tryNum = tryNum + 1;
							errorMsg = "as_connect_message_error";
							errorMsg_detail = "port not available";
							break;
						case odbc_Dcs_GetObjRefHdl_exc_.odbc_Dcs_GetObjRefHdl_ASNoSrvrHdl_exn_:
							done = false;
							tryNum = tryNum + 1;
							errorMsg = "as_connect_message_error";
							errorMsg_detail =
									"server handle not available. " + cr.m_p1_exception.ErrorText;
                            try {
                                Thread.sleep(1000);
                            } catch (InterruptedException e) {
                            }
                            break;
						default:
							//
							// Some unknown error
							//
							if (cr.m_p1_exception.clientErrorText != null) {
								errorText =
										"Client Error text = " + cr.m_p1_exception.clientErrorText;
							}
							errorText =
									errorText + "  :Exception = " + cr.m_p1_exception.exception_nr;
							errorText = errorText + "  :" + "Exception detail = "
									+ cr.m_p1_exception.exception_detail;
							errorText =
									errorText + "  :" + "Error code = "
											+ cr.m_p1_exception.errorCode;

							if (cr.m_p1_exception.ErrorText != null) {
								errorText = errorText + "  :" + "Error text = "
										+ cr.m_p1_exception.ErrorText;

							}
							throw TrafT4Messages.createSQLException(getT4props(),
									"as_connect_message_error", errorText);
					}
					if (!done && getT4props().isLogEnable(Level.FINER)) {
						String temp = "getObjRef Failed. Message from Association Server: "
								+ errorMsg_detail;
						T4LoggingUtilities.log(getT4props(), Level.FINER, temp);
					}
					if (LOG.isDebugEnabled()) {
						LOG.debug("getObjRef Failed. Message from Association Server: {}",
								errorMsg_detail);
					}
					currentTime = System.currentTimeMillis();
				} while (done == false && endTime > currentTime && tryNum < retryCount);

				if (done == false) {
					SQLException se1;
					SQLException se2;

					if (currentTime >= endTime) {
						se1 = TrafT4Messages
								.createSQLException(getT4props(), "ids_s1_t00", getTargetServer(),
										getLoginTimeout());
						se2 = TrafT4Messages
								.createSQLException(getT4props(), errorMsg, errorMsg_detail);
						se1.setNextException(se2);
					} else {
						se1 = TrafT4Messages
								.createSQLException(getT4props(), errorMsg, errorMsg_detail);
					}

					throw se1;
				}

				dialogueId = cr.m_p3_dialogueId;
				m_ncsSrvrRef = cr.m_p2_srvrObjRef;
				remoteProcess = "\\" + cr.remoteHost + "." + cr.remoteProcess;
				getT4props().setDialogueID(Integer.toString(dialogueId));
				getT4props().setServerID(m_ncsSrvrRef);
				getT4props().setRemoteProcess(remoteProcess);
				ncsAddr = cr.getNCSAddress();
				this.byteSwap = cr.byteSwap;
				this._serverDataSource = cr.m_p4_dataSource;

				setISOMapping(cr.isoMapping);

				if (cr.isoMapping == InterfaceUtilities.getCharsetValue("ISO8859_1")) {
					this.inContext.ctxDataLang = 0;
					this.inContext.ctxErrorLang = 0;
				}
				this.conn.checkLoginTimeout(getT4props());
				boolean initDiag = false;
				TrafT4Exception exception = null;
				try {
					if (cr.securityEnabledBase64) {
						this.base64EncryptPassword();
						initDiag = this.initDiag(false, false);
					} else if (cr.securityEnabled) {
						initDiag = this.secureLogin(cr);
					} else {
						this.oldEncryptPassword();
						initDiag = this.initDiag(false, false);
					}
				} catch (Exception e) {
					if (e instanceof SocketException || e instanceof TrafT4Exception) {
						if (LOG.isErrorEnabled()) {
							LOG.error(e.getMessage(), e);
						}
						if (e instanceof TrafT4Exception) {
							exception = (TrafT4Exception) e;
							if (exception.getVendorCode() == -8837)
								throw exception;
						}
						reallocate = true;
						reallocateNum++;
						if (reallocateNum == retryCount) {
							throw exception;
						}
						continue;
					} else {
						throw new SQLException(e.getMessage());
					}
				}
				reallocate = !initDiag;
				reallocateNum++;
			} while (reallocate && reallocateNum < retryCount);

			if (reallocate) {
				SQLException se = null;
				if (reallocateNum >= retryCount) {
					se = TrafT4Messages.createSQLException(getT4props(), "as_connect_message_error",
							"exceeded retry count");
				} else {
					se = TrafT4Messages
							.createSQLException(getT4props(), "as_connect_message_error");
				}

				throw se;
			}
			success = true;
		} catch (SQLException se) {
			boolean connectionError = this.conn.performConnectionErrorChecks(se);
			//if(addresses.size() == 1 || !connectionError) {
			//	throw se;
			//}
			if (connException == null) {
				connException = se;
			}
			int errorCode = se.getErrorCode();
			if (this.conn.ERROR_SOCKET_OPEN_CODE != errorCode) {
				connException = se;
			}

			if (errorCode == this.conn.TENANT_INVALID
					|| errorCode == this.conn.SQL_ERR_CLI_AUTH) {
				connException = se;
				return true;
			}
		}
		//this.setConnectionAttr(this._t4Conn, TRANSPORT.SQL_ATTR_CLIPVARCHAR, this.inContext.clipVarchar, String.valueOf(this.inContext.clipVarchar));
		return success;
	}

	// @deprecated
	void isConnectionClosed() throws SQLException {
		if (isClosed_ == false) {
			throw TrafT4Messages.createSQLException(getT4props(), "invalid_connection");
		}
	}

	// @deprecated
	void isConnectionOpen() throws SQLException {
		if (isClosed_) {
			throw TrafT4Messages.createSQLException(getT4props(), "invalid_connection");
		}
	}

	// @deprecated
	boolean getIsClosed() {
		return isClosed_;
	}

	void setIsClosed(boolean isClosed) {
		this.isClosed_ = isClosed;
	}

	String getTargetServer() {
		return m_ncsSrvrRef;
	}

	void setCatalog(TrafT4Connection conn, String catalog) throws SQLException {
	    if (getT4props().isLogEnable(Level.FINEST)) {
	        T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", catalog);
	    }
	    if (LOG.isTraceEnabled()) {
	        LOG.trace("ENTRY. {}", catalog);
	    }
	    if (catalog != null && catalog.length() == 0) {
	        catalog = T4Properties.DEFAULT_CATALOG;
	    }
		setConnectionAttr(conn, SQL_ATTR_CURRENT_CATALOG, 0, catalog);
		outContext.catalog = catalog;
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "LEAVE", catalog);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("LEAVE. {}", catalog);
        }
	};

	void setSessionDebug(TrafT4Connection conn, boolean sessionDebugEnable) throws SQLException {
		if (getT4props().isLogEnable(Level.FINEST)) {
			T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", sessionDebugEnable);
		}
		if (LOG.isTraceEnabled()) {
			LOG.trace("ENTRY. {}", sessionDebugEnable);
		}
		if(sessionDebugEnable)
			setConnectionAttr(conn, SET_SESSION_DEBUG, 1, "");
		else
			setConnectionAttr(conn, SET_SESSION_DEBUG, 0, "");
		if (getT4props().isLogEnable(Level.FINEST)) {
			T4LoggingUtilities.log(getT4props(), Level.FINEST, "LEAVE", sessionDebugEnable);
		}
		if (LOG.isTraceEnabled()) {
			LOG.trace("LEAVE. {}", sessionDebugEnable);
		}
	};

	void setClipVarchar(TrafT4Connection conn, int clipVarchar) throws SQLException {
		if (getT4props().isLogEnable(Level.FINEST)) {
			T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", clipVarchar);
		}
		if (LOG.isTraceEnabled()) {
			LOG.trace("ENTRY. {}", clipVarchar);
		}
		getT4props().setClipVarchar(clipVarchar);
		setConnectionAttr(conn, SET_CLIPVARCHAR, clipVarchar, "");
		if (getT4props().isLogEnable(Level.FINEST)) {
			T4LoggingUtilities.log(getT4props(), Level.FINEST, "LEAVE", clipVarchar);
		}
		if (LOG.isTraceEnabled()) {
			LOG.trace("LEAVE. {}", clipVarchar);
		}
	};

	// enforces the connection timeout set by the user
	// to be called by the connection pooling mechanism whenever a connection is
	// given to the user from the pool
	void enforceT4ConnectionTimeout(TrafT4Connection conn) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", getT4props().getConnectionTimeout());
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", getT4props().getConnectionTimeout());
        }
		inContext.idleTimeoutSec = (short) getT4props().getConnectionTimeout();
		setConnectionAttr(conn, JDBC_ATTR_CONN_IDLE_TIMEOUT, inContext.idleTimeoutSec, String
				.valueOf(inContext.idleTimeoutSec));
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "LEAVE", getT4props().getConnectionTimeout());
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("LEAVE. {}", getT4props().getConnectionTimeout());
        }
	};

	// disregards the T4's connectionTimeout value (set during initialize
	// dialog) and
	// enforces the connection timeout set by the NCS datasource settings
	// to be called by the connection pooling mechanism whenever a connection is
	// put into the pool (after a user has called connection.close())
	void disregardT4ConnectionTimeout(TrafT4Connection conn) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		setConnectionAttr(conn, JDBC_ATTR_CONN_IDLE_TIMEOUT, -1, "-1");
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "LEAVE");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("LEAVE");
        }
	};

	void setConnectionAttr(TrafT4Connection conn, short attr, int valueNum, String valueString) throws SQLException {
		SetConnectionOptionReply scr_;
		isConnectionOpen();

		try {
			scr_ = t4connection_.SetConnectionOption(attr, valueNum, valueString);
			//3196 - NDCS transaction for SPJ
			if (attr == SQL_ATTR_JOIN_UDR_TRANSACTION) {
				transId_ = Long.valueOf(valueString);
				suspendRequest_ = true;
			}
			else if (attr == SQL_ATTR_SUSPEND_UDR_TRANSACTION) {
				transId_ = Long.valueOf(valueString);
				suspendRequest_ = false;
			}
		} catch (SQLException tex) {

            if (getT4props().isLogEnable(Level.FINER)) {
                String temp = "NDCS or SQLException occurred." + tex.getMessage();
                T4LoggingUtilities.log(getT4props(), Level.FINER, temp, attr, valueNum, valueString);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("NDCS or SQLException occurred. {}, {}, {}, {}", tex.getMessage(), attr,
                        valueNum, valueString);
            }
			throw tex;
		}

		switch (scr_.m_p1.exception_nr) {
		case TRANSPORT.CEE_SUCCESS:

			// do the warning processing
			if (scr_.m_p2.length != 0) {
				TrafT4Messages.setSQLWarning(conn.getT4props(), conn, scr_.m_p2);
			}
            if (getT4props().isLogEnable(Level.FINER)) {
                String temp = "Setting connection attribute success.";
                T4LoggingUtilities.log(getT4props(), Level.FINER, temp, attr, valueNum, valueString);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Setting connection attribute success, {}, {}, {}", attr, valueNum,
                        valueString);
            }
			break;
		case odbc_SQLSvc_SetConnectionOption_exc_.odbc_SQLSvc_SetConnectionOption_SQLError_exn_:
            if (getT4props().isLogEnable(Level.FINER)) {
                String temp = "odbc_SQLSvc_SetConnectionOption_SQLError_exn_ occurred.";
                T4LoggingUtilities.log(getT4props(), Level.FINER, temp, attr, valueNum, valueString);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("odbc_SQLSvc_SetConnectionOption_SQLError_exn_ occurred, {}, {}, {}",
                        attr, valueNum, valueString);
            }
			TrafT4Messages.throwSQLException(getT4props(), scr_.m_p1.errorList);
		default:
            if (getT4props().isLogEnable(Level.FINER)) {
                String temp = "UnknownException occurred.";
                T4LoggingUtilities.log(getT4props(), Level.FINER, temp, attr, valueNum, valueString);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("UnknownException occurred, {}, {}, {}", attr, valueNum, valueString);
            }
			throw TrafT4Messages.createSQLException(conn.getT4props(), "ids_unknown_reply_error");
		}
	};


	void setTransactionIsolation(TrafT4Connection conn, int level) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            String temp = "Setting transaction isolation = " + level;
            T4LoggingUtilities.log(getT4props(), Level.FINEST, temp);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("Setting transaction isolation = {}", level);
        }
		isConnectionOpen();

		if (level != Connection.TRANSACTION_NONE && level != Connection.TRANSACTION_READ_COMMITTED
				&& level != Connection.TRANSACTION_READ_UNCOMMITTED && level != Connection.TRANSACTION_REPEATABLE_READ
				&& level != Connection.TRANSACTION_SERIALIZABLE) {
			throw TrafT4Messages.createSQLException(conn.getT4props(), "invalid_transaction_isolation");
		}

		switch (level) {
		case Connection.TRANSACTION_NONE:
			inContext.txnIsolationLevel = (short) SQL_TXN_READ_COMMITTED;
			break;
		case Connection.TRANSACTION_READ_COMMITTED:
			inContext.txnIsolationLevel = (short) SQL_TXN_READ_COMMITTED;
			break;
		case Connection.TRANSACTION_READ_UNCOMMITTED:
			inContext.txnIsolationLevel = (short) SQL_TXN_READ_UNCOMMITTED;
			break;
		case Connection.TRANSACTION_REPEATABLE_READ:
			inContext.txnIsolationLevel = (short) SQL_TXN_REPEATABLE_READ;
			break;
		case Connection.TRANSACTION_SERIALIZABLE:
			inContext.txnIsolationLevel = (short) SQL_TXN_SERIALIZABLE;
			break;
		default:
			inContext.txnIsolationLevel = (short) SQL_TXN_READ_COMMITTED;
			break;
		}

		setConnectionAttr(conn, SQL_TXN_ISOLATION, inContext.txnIsolationLevel, String
				.valueOf(inContext.txnIsolationLevel));
		txnIsolationLevel = level;
        if (getT4props().isLogEnable(Level.FINER)) {
            String temp = "Setting transaction isolation = " + level + " is done.";
            T4LoggingUtilities.log(getT4props(), Level.FINER, temp);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Setting transaction isolation = {} is done.", level);
        }
	};

	int getTransactionIsolation() throws SQLException {
		return txnIsolationLevel;
	}

	long getTxid() {
		return txid;
	}

	void setTxid(long txid) {
		this.txid = txid;
	}

	boolean getAutoCommit() {
		return autoCommit;
	}

    void setAutoCommit(TrafT4Connection conn, boolean autoCommit) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", autoCommit);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", autoCommit);
        }
        isConnectionOpen();

        if (autoCommit == false) {
            inContext.autoCommit = 0;
        } else {
            inContext.autoCommit = 1;
        }
        try {
            setConnectionAttr(conn, SQL_ATTR_AUTOCOMMIT, inContext.autoCommit, String.valueOf(inContext.autoCommit));
            this.autoCommit = autoCommit;
        } catch (SQLException sqle) {
            throw sqle;
        }
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "LEAVE", autoCommit);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("LEAVE. {}", autoCommit);
        }
    }

    void enableNARSupport(TrafT4Connection conn, boolean NARSupport) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", NARSupport);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", NARSupport);
        }
        int val = NARSupport ? 1 : 0;
        setConnectionAttr(conn, TRANSPORT.SQL_ATTR_ROWSET_RECOVERY, val, String.valueOf(val));
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "LEAVE", NARSupport);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("LEAVE. {}", NARSupport);
        }
    }

    void enableProxySyntax(TrafT4Connection conn) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        setConnectionAttr(conn, InterfaceConnection.SPJ_ENABLE_PROXY, 1, "1");
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "LEAVE");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("LEAVE");
        }
    }

	boolean isReadOnly() {
		return isReadOnly;
	}

	@Deprecated
	private void setReadOnly(boolean readOnly) throws SQLException {
		isConnectionOpen();
		this.isReadOnly = readOnly;
	}

    void setReadOnly(TrafT4Connection conn, boolean readOnly) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", readOnly);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", readOnly);
        }

        isConnectionOpen();
        if (readOnly == false) {
            inContext.accessMode = 0;
        } else {
            inContext.accessMode = 1;
        }
        setConnectionAttr(conn, SQL_ATTR_ACCESS_MODE, inContext.accessMode, String.valueOf(inContext.accessMode));
        this.isReadOnly = readOnly;

        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "LEAVE", readOnly);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("LEAVE. {}", readOnly);
        }
    }

	void close() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        TerminateDialogueReply tdr_ = null;
		//3196 - NDCS transaction for SPJ
		if (suspendRequest_) {
			this.conn.suspendUDRTransaction();
		}
		
		SecPwd.removeInstance(this.conn);
		
		try {
			tdr_ = t4connection_.TerminateDialogue();
		} catch (SQLException tex) {
            if (getT4props().isLogEnable(Level.FINER)) {
                String temp = "SQLException during TerminateDialogue.";
                T4LoggingUtilities.log(getT4props(), Level.FINER, temp);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("SQLException during TerminateDialogue.");
            }
			throw tex;
		}

		switch (tdr_.m_p1.exception_nr) {
		case TRANSPORT.CEE_SUCCESS:
			break;
		case odbc_SQLSvc_TerminateDialogue_exc_.odbc_SQLSvc_TerminateDialogue_SQLError_exn_:
			//ignore errors
		}

        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "LEAVE");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("LEAVE");
        }
	}

	void endTransaction(short commitOption) throws SQLException {
		EndTransactionReply etr_ = null;
		if (autoCommit) { 
			throw TrafT4Messages.createSQLException(getT4props(), "invalid_commit_mode");
		}

		isConnectionOpen();
		try {
			etr_ = t4connection_.endTransaction(commitOption);
		} catch (SQLException tex) {
            if (getT4props().isLogEnable(Level.FINER)) {
                String temp = "SQLException during EndTransaction." + tex.toString();
                T4LoggingUtilities.log(getT4props(), Level.FINER, temp, commitOption);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("SQLException during EndTransaction. {}, {}", tex.toString(),
                        commitOption);
            }
			throw tex;
		}

        if (etr_.shouldReconn()) {
            setReconn(true);
        }

		switch (etr_.m_p1.exception_nr) {
		case TRANSPORT.CEE_SUCCESS:
			break;
		case odbc_SQLSvc_EndTransaction_exc_.odbc_SQLSvc_EndTransaction_ParamError_exn_:
            if (getT4props().isLogEnable(Level.FINER)) {
                String temp = "odbc_SQLSvc_EndTransaction_ParamError_exn_ :";
                T4LoggingUtilities.log(getT4props(), Level.FINER, temp, commitOption);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("odbc_SQLSvc_EndTransaction_ParamError_exn_ : {}", commitOption);
            }
			throw TrafT4Messages.createSQLException(getT4props(), "ParamError:" + etr_.m_p1.ParamError);
		case odbc_SQLSvc_EndTransaction_exc_.odbc_SQLSvc_EndTransaction_InvalidConnection_exn_:
            if (getT4props().isLogEnable(Level.FINER)) {
                String temp = "odbc_SQLSvc_EndTransaction_InvalidConnection_exn_:";
                T4LoggingUtilities.log(getT4props(), Level.FINER, temp, commitOption);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("odbc_SQLSvc_EndTransaction_InvalidConnection_exn_: {}",
                        commitOption);
            }
			throw new SQLException("odbc_SQLSvc_EndTransaction_InvalidConnection_exn", "HY100002", 10001);
		case odbc_SQLSvc_EndTransaction_exc_.odbc_SQLSvc_EndTransaction_SQLError_exn_:
            if (getT4props().isLogEnable(Level.FINER)) {
                String temp = "odbc_SQLSvc_EndTransaction_SQLError_exn_:" + etr_.m_p1.SQLError;
                T4LoggingUtilities.log(getT4props(), Level.FINER, temp, commitOption);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("odbc_SQLSvc_EndTransaction_SQLError_exn_: {}, {}",
                        etr_.m_p1.SQLError, commitOption);
            }
			TrafT4Messages.throwSQLException(getT4props(), etr_.m_p1.SQLError);
		case odbc_SQLSvc_EndTransaction_exc_.odbc_SQLSvc_EndTransaction_SQLInvalidHandle_exn_:
            if (getT4props().isLogEnable(Level.FINER)) {
                String temp = "odbc_SQLSvc_EndTransaction_SQLInvalidHandle_exn_:";
                T4LoggingUtilities.log(getT4props(), Level.FINER, temp, commitOption);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("odbc_SQLSvc_EndTransaction_SQLInvalidHandle_exn_: {}", commitOption);
            }
			throw new SQLException("odbc_SQLSvc_EndTransaction_SQLInvalidHandle_exn", "HY100004", 10001);
		case odbc_SQLSvc_EndTransaction_exc_.odbc_SQLSvc_EndTransaction_TransactionError_exn_:
            if (getT4props().isLogEnable(Level.FINER)) {
                String temp = "odbc_SQLSvc_EndTransaction_TransactionError_exn_:";
                T4LoggingUtilities.log(getT4props(), Level.FINER, temp, commitOption);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("odbc_SQLSvc_EndTransaction_TransactionError_exn_: {}", commitOption);
            }
			throw new SQLException("odbc_SQLSvc_EndTransaction_TransactionError_exn", "HY100005", 10001);
		default:
            if (getT4props().isLogEnable(Level.FINER)) {
                String temp = "UnknownError:";
                T4LoggingUtilities.log(getT4props(), Level.FINER, temp, commitOption);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("UnknownError: {}", commitOption);
            }
			throw new SQLException("Unknown Error during EndTransaction", "HY100001", 10001);
		}

	};

	long beginTransaction() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		isConnectionOpen();

		return txid;
	};

    void savepointOpt(short commitOption, String savepointName) throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", commitOption, savepointName);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}, {}", commitOption, savepointName);
        }
        SavepointReply reply = null;
        if (autoCommit) {
            throw TrafT4Messages.createSQLException(getT4props(), "invalid_commit_mode");
        }
        isConnectionOpen();
        try {
            reply = t4connection_.savepointOpt(commitOption, savepointName);
        } catch (SQLException tex) {
            if (getT4props().isLogEnable(Level.FINER)) {
                String temp = "SQLException during Savepoint." + tex.toString();
                T4LoggingUtilities.log(getT4props(), Level.FINER, temp, commitOption, savepointName);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("SQLException during Savepoint. {}, {}, {}", tex.toString(), commitOption,
                        savepointName);
            }
            throw tex;
        }
        switch (reply.m_p1.exception_nr) {
            case TRANSPORT.CEE_SUCCESS:
                break;
            case odbc_SQLSvc_Savepoint_exc_.odbc_SQLSvc_Savepoint_SQLError_exn_:
                if (getT4props().isLogEnable(Level.FINER)) {
                    String temp = "odbc_SQLSvc_Savepoint_SQLError_exn_:" + reply.m_p1.SQLError;
                    T4LoggingUtilities.log(getT4props(), Level.FINER, temp, commitOption, savepointName);
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("odbc_SQLSvc_Savepoint_SQLError_exn_: {}, {}, {}",
                            reply.m_p1.SQLError, commitOption, savepointName);
                }
                TrafT4Messages.throwSQLException(getT4props(), reply.m_p1.SQLError);
            case odbc_SQLSvc_Savepoint_exc_.odbc_SQLSvc_Savepoint_SavepointError_exn_:
                if (getT4props().isLogEnable(Level.FINER)) {
                    String temp = "odbc_SQLSvc_Savepoint_SavepointError_exn_:";
                    T4LoggingUtilities.log(getT4props(), Level.FINER, temp, commitOption, savepointName);
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("odbc_SQLSvc_Savepoint_SavepointError_exn_: {}, {}", commitOption,
                            savepointName);
                }
                TrafT4Messages.throwSQLException(getT4props(), reply.m_p1.SQLError);
            default:
                if (getT4props().isLogEnable(Level.FINER)) {
                    String temp = "UnknownError:";
                    T4LoggingUtilities.log(getT4props(), Level.FINER, temp, commitOption, savepointName);
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("UnknownError: {}, {}", commitOption, savepointName);
                }
                throw new SQLException("Unknown Error during Savepoint", "HY100001", 10001);
        }
    };

    void reuse() {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
        txnIsolationLevel = Connection.TRANSACTION_READ_COMMITTED;
        autoCommit = true;
        isReadOnly = false;
        isClosed_ = false;
        txid = 0;
        t4connection_.reuse();
    };

    boolean useArrayBinding() {
        return useArrayBinding_;
    }

    short getTransportBufferSize() {
        return transportBufferSize_;
    }

    // methods for handling weak connections
    void removeElement(TrafT4Connection conn) {
        refTosrvrCtxHandle_.remove(conn.pRef_);
        conn.pRef_.clear();
	}

	void gcConnections() {
		Reference pRef;
		InterfaceConnection ic;
		while ((pRef = refQ_.poll()) != null) {
			ic = (InterfaceConnection) refTosrvrCtxHandle_.get(pRef);
			// All PreparedStatement objects are added to Hashtable
			// Only Statement objects that produces ResultSet are added to
			// Hashtable
			// Hence stmtLabel could be null
			if (ic != null) {
				try {
					ic.close();
				} catch (SQLException e) {
				} finally {
					refTosrvrCtxHandle_.remove(pRef);
				}
			}
		}
	}

	protected byte[] encodeString(String str, int charset) throws CharacterCodingException, UnsupportedCharsetException {
		Integer key = new Integer(charset);
		CharsetEncoder ce;
		byte[] ret = null;

		if (str != null) {
			if (this.isoMapping_ == InterfaceUtilities.SQLCHARSETCODE_ISO88591 && !this.enforceISO) {
				ret = str.getBytes(); //convert the old way
			} else {
				if ((ce = (CharsetEncoder) encoders.get(key)) == null) { //only create a new encoder if its the first time
					String charsetName = InterfaceUtilities.getCharsetName(charset);
					
					//encoder needs to be based on our current swap flag for UTF-16 data
					//this should be redesigned when we fixup character set issues for SQ
					if(key == InterfaceUtilities.SQLCHARSETCODE_UNICODE && this.byteSwap == true) {
						charsetName = "UTF-16LE";
					}
					
					Charset c = Charset.forName(charsetName);
					ce = c.newEncoder();
					ce.onUnmappableCharacter(CodingErrorAction.REPORT);
					encoders.put(key, ce);
				}
				
				synchronized(ce) { //since one connection shares encoders
					ce.reset();
					ByteBuffer buf = ce.encode(CharBuffer.wrap(str));
					ret = new byte[buf.remaining()];
					buf.get(ret, 0, ret.length);
				}
			}
		}

		return ret;
	}

	protected String decodeBytes(byte[] data, int charset) throws CharacterCodingException, UnsupportedCharsetException {
		Integer key = new Integer(charset);
		CharsetDecoder cd;
		String str = null;

		// we use this function for USC2 columns as well and we do NOT want to
		// apply full pass-thru mode for them
		if (this.isoMapping_ == InterfaceUtilities.SQLCHARSETCODE_ISO88591 && !this.enforceISO
				&& charset != InterfaceUtilities.SQLCHARSETCODE_UNICODE) {
			str = new String(data);
		} else {
			// the following is a fix for JDK 1.4.2 and MS932. For some reason
			// it does not handle single byte entries properly
			boolean fix = false;
			if (charset == 10 && data.length == 1) {
				data = new byte[] { 0, data[0] };
				fix = true;
			}

			if ((cd = (CharsetDecoder) decoders.get(key)) == null) { //only create a decoder if its the first time
				String charsetName = InterfaceUtilities.getCharsetName(charset);
				
				//encoder needs to be based on our current swap flag for UTF-16 data
				//this should be redesigned when we fixup character set issues for SQ
				if(key == InterfaceUtilities.SQLCHARSETCODE_UNICODE && this.byteSwap == true) {
					charsetName = "UTF-16LE";
				}
				
				Charset c = Charset.forName(charsetName);
				cd = c.newDecoder();
				cd.replaceWith(this.getT4props().getReplacementString());
				cd.onUnmappableCharacter(CodingErrorAction.REPLACE);
				decoders.put(key, cd);
			}
			
			synchronized(cd) { //one decoder for the entire connection
				cd.reset();
				str = cd.decode(ByteBuffer.wrap(data)).toString();
			}

			if (fix)
				str = str.substring(1);
		}

		return str;
	}

	protected String getApplicationName() {
		return this.getT4props().getApplicationName();
	}

	protected String getClientSocketAddress() {
		return clientSocketAddress;
	}

	protected void setClientSocketAddress(String clientSocketAddress) {
		this.clientSocketAddress = clientSocketAddress;
	}
	protected ConnectReply get_connectReply() {
		return _connectReply;
	}

	protected void set_connectReply(ConnectReply _connectReply) {
		this._connectReply = _connectReply;
	}
	
	void setClientInfoProperties(Properties prop) {
		this.getT4props().setClientInfoProperties(prop);
	}

	Properties getClientInfoProperties() {
		return this.getT4props().getClientInfoProperties();
	}

	protected void setClientInfo(String name, String value) throws SQLClientInfoException {
		this.getT4props().getClientInfoProperties().setProperty(name, value);
	}

	String getClientInfo(String name) {
		return this.getT4props().getClientInfoProperties().getProperty(name);
	}

    public boolean isProcessing() {
        return processing;
    }

    public void setProcessing(boolean processing) {
        this.processing = processing;
    }

    /**
     *  For reconnect feature, when driver conn to another mxosrvr,
     *  socket & related initDialReply will change.
     *  the following variables will change:
     *      dialogueId
     *      m_ncsSrvrRef
     *      remoteProcess
     *      ncsAddr
     */

    protected void setReconn(boolean srState) {
        reconn = srState;
    }
    protected boolean shouldReconn() {
        return reconn;
    }
    
    private String qid_;
    private String sqlString_;
    
    protected void setQid(String qid) {
        this.qid_ = qid;
        cancelFlag_ = false;
    }

    protected String getQid() {
        return qid_;
    }

	protected void setSqlString(String sqlString) {
		this.sqlString_ = sqlString;
	}

	protected String getSqlString() {
		return sqlString_;
	}

    private boolean cancelFlag_ = false;
    protected boolean getCancelFlag() {
        return cancelFlag_;
    }

    
    protected void setReconnNum(int reconnNum) {
        this.reconnNum = reconnNum;
    }
    
    protected void reconnIfNeeded() throws SQLException {
        if (shouldReconn() && checkRestore()) {
            if (getT4props().isLogEnable(Level.FINER)) {
                T4LoggingUtilities.log(getT4props(), Level.FINER, "Do reconnect.");
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("Do reconnect.");
            }
            String schema = getSchema();
            boolean autocommit = getAutoCommit();
            ConcurrentHashMap<Reference, Object[]> oldRefToStmt = new ConcurrentHashMap<Reference, Object[]>();
            oldRefToStmt.putAll(this.conn.refToStmt_);
            this.preClose();
            this.conn.refToStmt_.clear();
            try {
                this.reInitIc();
                this.conn.ic_ = this;
                this.connect();
            } catch (Exception se) {
                this.conn.checkLoginTimeout(t4props);
                if (getT4props().isLogEnable(Level.FINEST)) {
                    T4LoggingUtilities.log(getT4props(), Level.FINER, "reconn failed.", se);
                }
                if (LOG.isTraceEnabled()) {
                    LOG.trace("reconn failed. {}", se);
                }
                throw new SQLException(se.getMessage());
            } 
            setSchema(this.conn, schema);
            setAutoCommit(this.conn, autocommit);
            updateStmtConnContext(oldRefToStmt);
            for (Entry<Reference, Object[]> entry : oldRefToStmt.entrySet()) {
                Reference tmpRef = entry.getKey();
                TrafT4Statement tmpPstmt = (TrafT4Statement) tmpRef.get();
                if(tmpPstmt instanceof TrafT4PreparedStatement){
                    if((Boolean) entry.getValue()[1]){
                        TrafT4PreparedStatement pstmt = (TrafT4PreparedStatement) tmpPstmt;
                        pstmt.updatePstmtIc(this);
                        try {
                            pstmt.prepare(pstmt.sql_, pstmt.queryTimeout_, pstmt.resultSetHoldability_);
                            this.conn.refToStmt_.put(tmpRef, entry.getValue());
                        } catch (SQLException e) {
                            if(pstmt != null && !pstmt.isClosed()){
                                pstmt.isClosed_ = true;
                            }
                        }
                    }
                }
            }
            //clear pstmtcache after reconn
            this.conn.clearPstmtCache();
            if (autocommit) {
                this.conn.resetCQD(this);
            }
        }
    }
    
    protected void reInitIc() throws SQLException {
        
        activeTimeBeforeCancel = -1;
        txnIsolationLevel = Connection.TRANSACTION_READ_COMMITTED;
        autoCommit = true;
        isReadOnly = false;
        isoMapping_ = 15;
        termCharset_ = 0;
        enforceISO = false;
        byteSwap = false;
        transId_ = 0;
        suspendRequest_ = false;
        reconnNum = 1;
        
        t4props = this.conn.getT4props();
        remoteProcess = "";
        // close any weak connections that need to be closed.
        gcConnections();

        if (t4props.getSQLException() != null) {
            throw TrafT4Messages.createSQLException(getT4props(), "invalid_property", t4props
                .getSQLException());
        }

        m_sessionName = getT4props().getSessionName();

        if (m_sessionName != null && m_sessionName.length() > 0) {
            if (m_sessionName.length() > 24)
                m_sessionName = m_sessionName.substring(0, 24);

            if (!m_sessionName.matches("\\w+"))
                throw TrafT4Messages.createSQLException(getT4props(), "Invalid sessionName.  Session names can only contain alphnumeric characters.");
        }

        locale = t4props.getLocale();
        txid = 0;
        isClosed_ = false;
        useArrayBinding_ = t4props.getUseArrayBinding();
        // transportBufferSize_ = t4props.getTransportBufferSize();
        transportBufferSize_ = 32000;

        userDesc = getUserDescription(t4props.getUser());

        // Connection context details
        inContext = getInContext(t4props);
        m_ncsSrvrRef = t4props.getUrl();
        _ignoreCancel = t4props.getIgnoreCancel();

        sqlwarning_ = null;

        // if clientCharset is not set, use column definition
        this.termCharset_ = t4props.getClientCharset() == null ? termCharset_
                : InterfaceUtilities.getCharsetValue(t4props.getClientCharset());

        this.reconn = false;
    }
    
    protected void updateStmtConnContext(ConcurrentHashMap<Reference, Object[]> conMap){
        for (Entry<Reference, Object[]> entry : conMap.entrySet()) {
            Reference ref = entry.getKey();
            TrafT4Statement stmt = (TrafT4Statement) ref.get();
            stmt.updateStmtIc(this);
            for (TrafT4ResultSet resultset : stmt.resultSet_) {
                if (resultset != null && resultset.isClosed_ == false) {
                    resultset.updateResultSetIc(this);
                }
            }
        }
    }
    
    protected boolean checkRestore(){
        T4Connection t4connection = this.getT4Connection();
        boolean res = false;
        try {
            LogicalByteArray wbuffer = RestoreMessage.marshal(this.getDialogueId(), "RESTORE", this);
            LogicalByteArray rbuffer = t4connection.getReadBuffer(TRANSPORT.SRVR_API_TORESTORE, wbuffer);
            RestoreReply rr = new RestoreReply(rbuffer, this, reconnNum);
            res = rr.isRsFlag();
        } catch (UnsupportedCharsetException e) {
            if (getT4props().isLogEnable(Level.FINEST)) {
                T4LoggingUtilities.log(getT4props(), Level.FINER, "check restore failed.", e);
            }
            if (LOG.isTraceEnabled()) {
                LOG.trace("check restore failed. {}", e);
            }
        } catch ( CharacterCodingException e) {
			if (getT4props().isLogEnable(Level.FINEST)) {
				T4LoggingUtilities.log(getT4props(), Level.FINER, "check restore failed.", e);
			}
			if (LOG.isTraceEnabled()) {
				LOG.trace("check restore failed. {}", e);
			}
		} catch (SQLException e) {
			if (getT4props().isLogEnable(Level.FINEST)) {
				T4LoggingUtilities.log(getT4props(), Level.FINER, "check restore failed.", e);
			}
			if (LOG.isTraceEnabled()) {
				LOG.trace("check restore failed. {}", e);
			}
		}
        return res;
    }
    
    protected void preClose() {
        try {
            ArrayList<Object[]> stmts = new ArrayList<Object[]>(this.conn.refToStmt_.values());
            int size = stmts.size();
            for (int i = 0; i < size; i++) {
                try {
                    String stmtLabel = (String) stmts.get(i)[0];
                    TrafT4Statement stmt = new TrafT4Statement(this.conn, stmtLabel);
                    stmt.close();
                    stmt = null;
                } catch (SQLException se) {
                    // Ignore any exception and proceed to the next statement
                }
            }
            this.conn.refToStmt_.clear();

            if (this != null) {
                this.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }finally {
            if (this != null) {
                this.removeElement(this.conn);
            }
            if (this != null && this.getT4Connection() != null) {
                this.getT4Connection().closeTimers();
            }
        }
        
    }

    protected void storeCQDSql(short executeAPI, int paramRowCount, int paramCount,
        Object[] paramValues, int queryTimeout, String sql, TrafT4Statement stmt) {
        if (sql != null && !sql.equals("") && sql.length() > 3) {
            String tmp = sql.trim().substring(0, 3).toUpperCase();
            boolean res = tmp.equals("CQD");
            if (res) {
                if (getT4props().isLogEnable(Level.FINEST)) {
                    T4LoggingUtilities.log(getT4props(), Level.FINER, "reset cqd entry");
                }
                if (LOG.isTraceEnabled()) {
                    LOG.trace("reset cqd entry");
                }
                this.conn.storeCQDMsg(executeAPI, paramRowCount, paramCount, paramValues, queryTimeout,
                        sql, stmt);
            }
        }
    }

    private void doCancel(int srvrType, String srvrObjRef, int stopType) throws SQLException {
	    CancelReply cr_ = null;
		srvrObjRef += ":" + this.getSqlString();
	    if (getT4props().isLogEnable(Level.FINER)) {
		    String temp = "cancel request received for " + srvrObjRef;
		    T4LoggingUtilities.log(getT4props(), Level.FINER, temp);
	    }
	    if (LOG.isDebugEnabled()) {
		    LOG.debug("cancel request received for {}", srvrObjRef);
	    }
	    try {
		    cr_ = t4DcsConnection.cancel(srvrType, srvrObjRef, stopType);

	    } catch (SQLException e) {
		    if (!e.getMessage().contains("Socket is not connected") && e.getErrorCode() != -29231) {
			    throw e;
		    }
		    String t4Url = getT4props().getT4Url();
		    TrafCheckActiveMaster activeMaster = TrafCheckActiveMaster.getActiveMasterCacheMap().get(t4Url);
		    if (getT4props().isLogEnable(Level.FINER)) {
			    T4LoggingUtilities.log(getT4props(), Level.FINER, "ENTRY", activeMaster.getMasterList(), t4Url);
		    }
		    if (LOG.isDebugEnabled()) {
			    LOG.debug(
					    "ENTRY. Cancel Connetcion error, Try to retry... Master List :<{}> t4Url :<{}>",
					    activeMaster.getMasterList(), t4Url);
		    }
		    if (activeMaster.getMasterList() == null || activeMaster.getMasterList().size() == 0) {
			    if (LOG.isErrorEnabled()) {
				    LOG.error("Cancel Connetcion retry, Master List null, t4Url :<{}>", t4Url);
			    }
			    throw e;
		    } else {
			    for (int i = 0; i < activeMaster.getMasterList().size(); i++) {
				    try {
					    String url = T4Address
							    .extractUrlListFromUrl(t4Url, activeMaster.getMasterList().get(i));
					    T4DcsConnection t4DcsConnection = new T4DcsConnection(this, url);
					    cr_ = t4DcsConnection.cancel(srvrType, srvrObjRef, stopType);
					    if (getT4props().isLogEnable(Level.FINER)) {
						    T4LoggingUtilities.log(getT4props(), Level.FINER, "ENTRY", url);
					    }
					    if (LOG.isDebugEnabled()) {
						    LOG.debug("ENTRY. Cancel Connetcion retry successful...Url:<{}>", url);
					    }
					    break;
				    } catch (SQLException se) {
					    if (se.getErrorCode() == -29231) {
						    if (getT4props().isLogEnable(Level.FINER)) {
							    T4LoggingUtilities.log(getT4props(), Level.FINER,
									    "cancel time out and don't need to do anything");
						    }
						    if (LOG.isDebugEnabled()) {
							    LOG.debug("ccancel time out and don't need to do anything");
						    }
						    if (i == (activeMaster.getMasterList().size() - 1)) {
							    cancelFlag_ = true;
							    return;
						    }
						    continue;
					    }
					    if (i == (activeMaster.getMasterList().size() - 1)) {
						    throw se;
					    } else {
						    if (se.getMessage().contains("Socket is not connected")) {
							    if (getT4props().isLogEnable(Level.FINER)) {
								    String temp = "Socket is not connected";
								    T4LoggingUtilities.log(getT4props(), Level.FINER, temp);
							    }
							    if (LOG.isDebugEnabled()) {
								    LOG.debug("Socket is not connected and retry, errorMess : <{}>",
										    se.getMessage(), se);
							    }
						    } else {
							    throw se;
						    }
					    }
				    }
			    }
		    }
	    }
	    switch (cr_.m_p1_exception.exception_nr) {
		    case TRANSPORT.CEE_SUCCESS:
			    if (getT4props().isLogEnable(Level.FINER)) {
				    String temp = "Cancel successful";
				    T4LoggingUtilities.log(getT4props(), Level.FINER, temp);
			    }
			    if (LOG.isDebugEnabled()) {
				    LOG.debug("Cancel successful");
			    }
			    cancelFlag_ = true;
			    break;
		    default:
			    //
			    // Some unknown error
			    //
			    if (getT4props().isLogEnable(Level.FINER)) {
				    T4LoggingUtilities
						    .log(getT4props(), Level.FINER, cr_.m_p1_exception.toString());
			    }
			    if (LOG.isDebugEnabled()) {
				    LOG.debug("{}", cr_.m_p1_exception.toString());
			    }
			    throw TrafT4Messages.createSQLException(getT4props(), "as_cancel_message_error",
					    cr_.m_p1_exception.toString());
	    } // end switch
    }

    private int majorVersion;
    private int minorVersion;

    private void setMajorVersion(int majorVersion) {
        this.majorVersion = majorVersion;
    }

    private void setMinorVersion(int minorVersion) {
        this.minorVersion = minorVersion;
    }

    protected int getMajorVersion() {
        return this.majorVersion;
    }

    protected int getMinorVersion() {
        return this.minorVersion;
    }


    private short dcsVersion;
    protected void setDcsVersion(short dcsVersion){
        this.dcsVersion = dcsVersion;
        this.conn.setDcsVersion(dcsVersion);
    }

    protected short getDcsVersion(){
        return this.dcsVersion;
    }

}
