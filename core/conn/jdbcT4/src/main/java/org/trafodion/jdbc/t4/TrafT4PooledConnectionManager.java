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

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Vector;
import java.util.logging.Level;
import java.util.logging.LogRecord;

import javax.sql.ConnectionEvent;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;
import org.slf4j.LoggerFactory;

public class TrafT4PooledConnectionManager implements javax.sql.ConnectionEventListener {
    private final static org.slf4j.Logger LOG =
            LoggerFactory.getLogger(TrafT4PooledConnectionManager.class);

	public void connectionClosed(ConnectionEvent event) {
	    if (getT4props().isLogEnable(Level.FINEST)) {
	        T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", event);
	    }
	    if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", event);
        }
		PooledConnection pc;

		pc = (PooledConnection) event.getSource();

		boolean addToFreePool = true;
		if (minPoolSize_ > 0 && free_.size() >= minPoolSize_) {
			addToFreePool = false;
		}
		// If an initial pool is being created, then ensure that the connection
		// is
		// added to the free pool irrespective of minPoolSize being reached
		if (initialPoolCreationFlag_) {
			addToFreePool = true;
		}
		boolean wasPresent = removeInUseConnection(pc, addToFreePool);

		if (wasPresent && (!addToFreePool)) {
			try {
				pc.close();
			} catch (SQLException e) {
				// ignore any close error
			}
		}
	}

	public void connectionErrorOccurred(ConnectionEvent event) {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY", event);
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY. {}", event);
        }

		PooledConnection pc;

		pc = (PooledConnection) event.getSource();
		try {
			pc.close();
		} catch (SQLException e) {
			// ignore any close error
		}
		removeInUseConnection(pc, false);
	}

    public Connection getConnection() throws SQLException {
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }

        PooledConnection pc;
        boolean validConnection = false;

        do {
            if (free_.size() == 0) {
                if (maxPoolSize_ == 0 || count_ < maxPoolSize_) {
                    pc = pds_.getPooledConnection();
                    count_++;
                    pc.addConnectionEventListener(this);
                    inUse_.add(pc);

                    TrafT4Connection c = (TrafT4Connection) pc.getConnection();
                    try {
                        c.ic_.enforceT4ConnectionTimeout(c);
                        validConnection = true;
                    } catch (SQLException sqlEx) {
                        try {
                            pc.close();
                        } catch (SQLException e) {
                        } // cleanup, ignore any errors
                    }
                } else {
                    throw TrafT4Messages.createSQLException(getT4props(), "max_pool_size_reached",
                            maxPoolSize_);
                }
            } else {
                pc = (PooledConnection) free_.get(0);
                if (removeFreeConnection(pc, true)) {
                    TrafT4Connection c = (TrafT4Connection) pc.getConnection();
                    try {
                        c.ic_.enforceT4ConnectionTimeout(c);
                        validConnection = true;
                    } catch (SQLException sqlEx) {
                        try {
                            pc.close();
                        } catch (SQLException e) {
                        } // cleanup, ignore any errors
                        if (removeInUseConnection(pc, false)) {
                            --count_;
                        }
                    }
                }
            }
        } while (!validConnection);

        return pc.getConnection();
    }

	private synchronized boolean removeFreeConnection(PooledConnection pc, boolean addToUsePool) {
		boolean wasPresent = free_.remove(pc);
		hashTab_.remove(pc);
		if (wasPresent) {
			if (addToUsePool) {
				inUse_.add(pc);
			} else {
				count_--;
			}
		}
		return wasPresent;
	}

	private synchronized boolean removeInUseConnection(PooledConnection pc, boolean addToFreePool) {
		boolean wasPresent = inUse_.remove(pc);
		hashTab_.remove(pc);
		if (wasPresent) {
			if (addToFreePool) {
				hashTab_.put(pc, new Long(System.currentTimeMillis() + (1000 * maxIdleTime_)));
				free_.add(pc);
			} else {
				count_--;
			}
		}
		return wasPresent;
	}

	private void createInitialPool(int initialPoolSize) throws SQLException {
		if (initialPoolSize <= 0) {
			return;
		}

		int limit = Math.min(initialPoolSize, maxPoolSize_);
		Connection initPool_[] = new Connection[limit];
		int created = 0;
		try {
			// Set initialPoolInCreation to indicate that an initial pool is in
			// the
			// process of being created.
			initialPoolCreationFlag_ = true;

			for (int i = 0; i < limit; i++) {
				initPool_[i] = getConnection();
				created++;
			}
		} catch (SQLException se) {
			SQLException head = TrafT4Messages.createSQLException(null, "initial_pool_creation_error", limit);
			head.setNextException(se);
			throw head;
		} finally {
			for (int i = 0; i < created; i++) {
				try {
					if (initPool_[i] != null)
						initPool_[i].close();
				} catch (SQLException se) {
					// ignore
				}
			}
			// Ensuring that the initialPoolInCreation has been set to false to
			// indicate
			// that the initial pool creation process has occured.
			initialPoolCreationFlag_ = false;
		}
	}

	void setLogWriter(PrintWriter out) {
		out_ = out;
	}

	TrafT4PooledConnectionManager(TrafT4ConnectionPoolDataSource pds, Level traceLevel) throws SQLException {
		String className = getClass().getName();
		t4props = pds;
		pds_ = pds;
		inUse_ = Collections.synchronizedList(new LinkedList());
		free_ = Collections.synchronizedList(new LinkedList());
		maxPoolSize_ = pds.getMaxPoolSize();
		minPoolSize_ = pds.getMinPoolSize();
		maxIdleTime_ = pds.getMaxIdleTime();
		connectionTimeout_ = pds.getConnectionTimeout();
		traceLevel_ = traceLevel;
		timer_ = null;
		if (maxIdleTime_ > 0 && maxPoolSize_ > 0) {
			IdleConnectionCleanupTask timerTask_ = new IdleConnectionCleanupTask();
			if(timer_ == null) {
				timer_ = new Timer(true);
			}
			timer_.schedule(timerTask_, (maxIdleTime_ * 1000), (maxIdleTime_ * 500));
		}
		if (connectionTimeout_ > 0 && maxPoolSize_ > 0) {
			ConnectionTimeoutCleanupTask timerTask_ = new ConnectionTimeoutCleanupTask();
			if (timer_ == null) {
				timer_ = new Timer(true);
			}
			timer_.schedule(timerTask_, (connectionTimeout_ * 1000), (connectionTimeout_ * 500));
		}
		try {
			createInitialPool(pds.getInitialPoolSize());
			traceId_ = "jdbcTrace:[" + Thread.currentThread() + "]:[" + hashCode() + "]:" + className + ".";
		} catch (SQLException sqle) {
			throw sqle;
		} finally {
			if(timer_ != null) {
				timer_.cancel();
			}
		}


	}

    T4Properties getT4props() {
        return t4props;
    }

	ConnectionPoolDataSource pds_;
	// LinkedList inUse_;
	// LinkedList free_;
	List inUse_;
	List free_;
	int count_;

	int maxPoolSize_;
	int minPoolSize_;
	long maxIdleTime_;
	int connectionTimeout_;
	Level traceLevel_;
	PrintWriter out_;
	String traceId_;
	Timer timer_;
	Hashtable hashTab_ = new java.util.Hashtable(); // synchronized
	// We keep a flag to indicate to this class that an initial pool is in the
	// process
	// of being created
	boolean initialPoolCreationFlag_ = false;
	T4Properties t4props;
	/*
	 * Private class used to clean up the connections that have surpassed
	 * maxIdleTime
	 */
	/* Start TimerTask definition */
	private class IdleConnectionCleanupTask extends TimerTask {
		Vector toRemove = null;

		IdleConnectionCleanupTask() {
			toRemove = new Vector();
		}

		public void run() {
			cleanUp();
		}

		private void cleanUp() {
			toRemove.clear();
			synchronized (free_) {
				try {
					Iterator it_ = free_.iterator();
					while (it_.hasNext()) {
						PooledConnection tempPC = (PooledConnection) it_.next();
						Long timeOutVal = (Long) hashTab_.get(tempPC);
						if (System.currentTimeMillis() > timeOutVal.longValue()) {
							toRemove.add(tempPC);
						}
					}
				} catch (Throwable t) {
                    if (getT4props().isLogEnable(Level.FINER)) {
                        T4LoggingUtilities.log(getT4props(), Level.FINER, t.getMessage());
                    }
                    if (LOG.isDebugEnabled()) {
                        LOG.debug("{}", t.getMessage());
                    }
				}
			} // synchronized block
			for (int i = 0; i < toRemove.size(); i++) {
				PooledConnection pc = (PooledConnection) toRemove.get(i);
				boolean wasPresent = removeFreeConnection(pc, false);
				if (wasPresent) {
					// close it to cleanup
					try {
						/*
						 * System.out.println("Closing connection : " + (
						 * (TrafT4Connection) ( (TrafT4PooledConnection)
						 * pc).getConnection()).getDialogueId());
						 */
						pc.close();
					} catch (SQLException se) {
						// Ignore
					}
				}
			}
		}
	}

	/* End TimerTask definition */
	/*
	 * Private class used to clean up the connections that have surpassed
	 * connectionTimeout
	 */
	/* Start TimerTask definition */
	private class ConnectionTimeoutCleanupTask extends TimerTask {
		Vector toRemove = null;

		ConnectionTimeoutCleanupTask() {
			toRemove = new Vector();
		}

		public void run() {
			cleanUp();
		}

		private void cleanUp() {
			toRemove.clear();
			synchronized (inUse_) {
				Iterator it_ = inUse_.iterator();
				while (it_.hasNext()) {
					try {
						PooledConnection tempPC = (PooledConnection) it_.next();
						InterfaceConnection ic = ((TrafT4PooledConnection) tempPC).getTrafT4ConnectionReference().ic_;
						if (ic != null) {
							T4Connection tconn = ic.getT4Connection();
							if (tconn != null) {
								if (tconn.connectionIdleTimeoutOccured()) {
									// System.out.println("********* Found a
									// timed out connection **********");
									toRemove.add(tempPC);
								}
							}
						}
					} catch (Throwable t) {
	                    if (getT4props().isLogEnable(Level.FINER)) {
	                        T4LoggingUtilities.log(getT4props(), Level.FINER, t.getMessage());
	                    }
	                    if (LOG.isDebugEnabled()) {
                            LOG.debug("{}", t.getMessage());
                        }
					}
				}
			} // synchronized block
			for (int i = 0; i < toRemove.size(); i++) {
				PooledConnection pc = (PooledConnection) toRemove.get(i);
				removeInUseConnection(pc, false);
				// do not close the connections because:
				// 1.> Corresponding NCS server is already gone
				// 2.> We need to give a timeout error when user uses this
				// connection
			}
		}
	}
	/* End TimerTask definition */

}
