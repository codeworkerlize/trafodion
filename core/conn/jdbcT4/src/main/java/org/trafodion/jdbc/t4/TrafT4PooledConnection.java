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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.LinkedList;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.LogRecord;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionEventListener;
import javax.sql.StatementEventListener;
import org.slf4j.LoggerFactory;

public class TrafT4PooledConnection implements javax.sql.PooledConnection {
    private final static org.slf4j.Logger LOG =
            LoggerFactory.getLogger(TrafT4PooledConnection.class);

	public void addConnectionEventListener(ConnectionEventListener listener) {
	    if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", listener);
        }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", listener);
        }
		if (isClosed_ || connection_ == null) {
			return;
		}
		listenerList_.add(listener);
	}

	public void close() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		
		//3196 - NDCS transaction for SPJ
		if (connection_.ic_.suspendRequest_) {
			connection_.suspendUDRTransaction();
		}
		
		if (isClosed_) {
			return;
		}
		connection_.close(true, true);
	}

	public Connection getConnection() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		if (isClosed_ || connection_ == null) {
			throw TrafT4Messages.createSQLException(getT4props(), "invalid_connection");
		}
		if (LogicalConnectionInUse_) {
			connection_.close(false, false);
		}
		LogicalConnectionInUse_ = true;
		connection_.reuse();
		return connection_;
	}

	public void removeConnectionEventListener(ConnectionEventListener listener) {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", listener);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", listener);
        }
		if (isClosed_ || connection_ == null) {
			return;
		}
		listenerList_.remove(listener);
	}

	// Called by TrafT4Connection when the connection is closed by the application
	void logicalClose(boolean sendEvents) {
		int i;
		int totalListener;
		ConnectionEventListener listener;

		LogicalConnectionInUse_ = false;
		
		try {
			//3196 - NDCS transaction for SPJ
			if (connection_.ic_.suspendRequest_) {
				connection_.suspendUDRTransaction();
			}
		}
		catch (SQLException ex) {}

		if (sendEvents) {
			totalListener = listenerList_.size();
			ConnectionEvent event = new ConnectionEvent(this);
			for (i = 0; i < totalListener; i++) {
				listener = (ConnectionEventListener) listenerList_.get(i);
				listener.connectionClosed(event);
			}
		}
	}

	void sendConnectionErrorEvent(SQLException ex) throws SQLException {
		int i;
		int totalListener;
		ConnectionEventListener listener;

		LogicalConnectionInUse_ = false;
		totalListener = listenerList_.size();
		ConnectionEvent event = new ConnectionEvent(this, ex);
		for (i = 0; i < totalListener; i++) {
			listener = (ConnectionEventListener) listenerList_.get(i);
			listener.connectionErrorOccurred(event);
		}
		close();
	}

	// Constructor
	TrafT4PooledConnection(TrafT4ConnectionPoolDataSource pds, T4Properties t4props) throws SQLException {
		super();

		T4Properties t4LocalProps;

		pds_ = pds;
		if (t4props != null) {
			t4LocalProps = t4props;
			locale_ = t4props.getLocale();
		} else {
			t4LocalProps = new T4Properties();
			locale_ = Locale.getDefault();
		}
		listenerList_ = new LinkedList();
		connection_ = new TrafT4Connection(this, t4LocalProps);
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "LEAVE", pds, t4props);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("LEAVE. {}, {}", pds, t4props);
        }
	}

    private T4Properties getT4props() {
        return connection_.getT4props();
    }

	TrafT4Connection getTrafT4ConnectionReference() {
		return connection_;
	}

	private LinkedList listenerList_;
	private boolean isClosed_ = false;
	private TrafT4ConnectionPoolDataSource pds_;
	private TrafT4Connection connection_;
	private Locale locale_;
	private boolean LogicalConnectionInUse_ = false;
	public void addStatementEventListener(StatementEventListener listener) {
		// TODO Auto-generated method stub
		
	}

	public void removeStatementEventListener(StatementEventListener listener) {
		// TODO Auto-generated method stub
		
	}
}
