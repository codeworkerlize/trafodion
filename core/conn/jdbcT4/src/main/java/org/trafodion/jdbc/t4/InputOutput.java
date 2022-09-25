
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

import java.io.*;
import java.net.InetSocketAddress;
import java.io.IOException;
import java.lang.ref.Reference;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.security.KeyStore;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.sql.SQLException;
import java.util.Vector;
import java.util.Map.Entry;
import java.util.logging.Level;

import javax.net.SocketFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import org.slf4j.LoggerFactory;
import org.trafodion.jdbc.t4.function.KeepAliveFun;
import org.trafodion.jdbc.t4.tmpl.Vproc;

class InputOutput {
	private Address m_addr;
	private Socket m_socket;
	private T4Properties m_t4props;
	private int readHdrLength = Header.sizeOf();
	private int m_dialogueId;
	private int m_timeout;
	private int m_connectionIdleTimeout;
	private Header m_rheader;
	private OutputStream m_os;
	private InputStream m_is;
	private WritableByteChannel m_wbc;
	private InterfaceConnection ic;
	//private int m_sendBufSize;

	private char compress = Header.NO;			//Header.NO means no compression is used.
	private char compType = Header.COMP_0;		//the used compression type. COMP_0 which means no compression

	private static SocketFactory m_factory = null; // NRV
	private static SocketFactory mxosrvr_factory = null;
	private static boolean isInitalized = false;
	private final static org.slf4j.Logger LOG = LoggerFactory.getLogger(InputOutput.class);
	// -
	// socket
	// factory

	private String clientSocketAddress;
    // This is minimum time the socket read and write will wait
    // All other timeouts will be multiple of this timeout
    private int m_networkTimeoutInMillis;

	protected void init(T4Properties props) throws Exception {
		synchronized (InputOutput.class) {
			if (isInitalized) {
				return;
			}
			try {
				String factStr = props.getSocketFactoryClasss();
				m_factory = SocketFactory.getDefault();
				if (factStr != null) {
					Class clazz = Class.forName(factStr);
					if (SocketFactory.class.isAssignableFrom(clazz)) {
						m_factory = (SocketFactory) clazz.newInstance();
					}
				}
			} catch (Throwable t) {
				m_factory = SocketFactory.getDefault();
			}
			if (props.isSSLEnabled()) {
				mxosrvr_factory = initSSLFactory(props);
			} else {
				mxosrvr_factory = m_factory;
			}
			isInitalized = true;
		}
	}

	private static SSLSocketFactory initSSLFactory(T4Properties props) throws Exception {
		boolean isTrustAllCerts = props.isTrustAllCerts();
		TrustManagerFactory trustManagerFactory = null;
		SSLContext sslContext = null;
		sslContext = SSLContext.getInstance(props.getSSLContextProtoco());
		TrustManager[] trustMgr = null;
		if (isTrustAllCerts) {
			trustMgr = new TrustManager[1];
			trustMgr[0] = new X509TrustManager() {
				public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
				}

				public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
				}

				public X509Certificate[] getAcceptedIssuers() {
					return null;
				}
			};
		} else {
			String jksPath = props.getJKSPath();
			if (jksPath == null || "".equals(jksPath.trim()) || !new File(jksPath).isFile()) {
				throw new Exception("File not exists. SSLJKSPath:" + jksPath);
			}
			String jksPwd = props.getJKSPwd();
			String trustManagerAlgorithm = props.getTrustManagerAlgorithm();
			String trustManagerProvider = props.getTrustManagerProvider();
			String keystoreType = props.getKeystoreType();
			if (jksPath != null && !jksPath.equalsIgnoreCase("")) {
				if (trustManagerProvider == null || "".equals(trustManagerProvider.trim()))
					trustManagerFactory = TrustManagerFactory.getInstance(trustManagerAlgorithm);
				else
					trustManagerFactory = TrustManagerFactory.getInstance(trustManagerAlgorithm, trustManagerProvider);

				KeyStore keyStore = KeyStore.getInstance(keystoreType);
				keyStore.load(new FileInputStream(jksPath), jksPwd == null ? null : jksPwd.toCharArray());

				trustManagerFactory.init(keyStore);
				trustMgr = trustManagerFactory.getTrustManagers();
			}
		}
		sslContext.init(null, trustMgr, null);
		return sslContext.getSocketFactory();
	}

	public static SocketFactory getSocketFactory() {
		return m_factory;
	}

	public static void setSocketFactory(SocketFactory factory) {
		m_factory = factory;
	}
	// ----------------------------------------------------------
	InputOutput(T4Properties t4props, Address addr1) {
		m_t4props = t4props;
		m_addr = addr1;
		m_dialogueId = 0;
		m_timeout = 0;
		m_connectionIdleTimeout = 0;
		m_networkTimeoutInMillis = t4props.getNetworkTimeoutInMillis();
		if(m_addr.m_t4props.getCompression()) {
			compress = Header.YES;
		}

		m_rheader = new Header((short) 0, (short) Vproc.jdbcMajorVersion, m_dialogueId, 0, 0, compress, compType, Header.READ_RESPONSE_FIRST,
				Header.SIGNATURE, Header.CLIENT_HEADER_VERSION_BE, Header.PC, Header.TCPIP, Header.NO);

	} // end InputOutput

	void setInterfaceConnection(InterfaceConnection ic) {
		this.ic = ic;
	}

	void setDialogueId(int dialogueId) {
		m_dialogueId = dialogueId;
	}

	void setTimeout(int timeout) {
		m_timeout = timeout;
	}

	void setConnectionIdleTimeout(int timeout) {
		m_connectionIdleTimeout = timeout;
	}

	void setNetworkTimeoutInMillis(int timeout) throws SQLException {
		int oldTimeout = m_networkTimeoutInMillis;
		if (timeout == 0)
			m_networkTimeoutInMillis = T4Properties.DEFAULT_NETWORK_TIMEOUT_IN_MILLIS;
		else
           		m_networkTimeoutInMillis = timeout; 
		if (m_socket != null && m_networkTimeoutInMillis != oldTimeout) { 
			try {
				m_socket.setSoTimeout(m_networkTimeoutInMillis);
			} catch (java.net.SocketException e) {
				throw new SQLException(e);
			}
		}
	}

	String getRemoteHost() {
		return this.m_addr.getIPorName();
	}

	// ----------------------------------------------------------
	synchronized void openIO() throws SQLException {
		// trace_connection - AM
        if (getT4props().isLogEnable(Level.FINEST)) {
            T4LoggingUtilities.log(getT4props(), Level.FINEST, "ENTRY");
        }
        if (LOG.isTraceEnabled()) {
            LOG.trace("ENTRY");
        }
		if (m_socket == null) {
			if(!isInitalized){
				try {
					init(this.m_addr.m_t4props);
				} catch (Exception e) {
					SQLException se = TrafT4Messages.createSQLException(m_t4props, "socket_open_error", e
							.getMessage());
					se.initCause(e);
					throw se;
				}
			}

			int numTry = 0;
			boolean found = false;
			int i = 0;
			Vector eList = new Vector();

			//
			// Sometimes the server isn't ready to be contacted, so we will try
			// 3 times
			// before giving up.
			//
			while (found == false && numTry < 3) {
				//
				// Because each machine name can have multiple IP addresses
				// associated with it,
				// we need to go through the entire list of IP addresses until
				// we can find
				// one we can connect too, or all address fail.
				//
				i = 0;
				while (found == false && i < m_addr.m_inetAddrs.length) {
					try {
						if (m_addr instanceof T4Address) {
							m_socket = m_factory.createSocket();
						} else {
							m_socket = mxosrvr_factory.createSocket();
						}
						int connectTimeout;
						if (m_timeout == 0)	
							connectTimeout = T4Properties.DEFAULT_CONNECT_TIMEOUT_IN_SECS * 1000; 
						else
							connectTimeout = m_timeout * 1000;
						m_socket.connect(new InetSocketAddress(m_addr.m_inetAddrs[i], m_addr.m_portNumber.intValue()), connectTimeout);
						m_socket.setKeepAlive(true);
						m_socket.setSoLinger(false, 0); // Make sure the socket can immediately reused if connection is lost.
						m_socket.setSoTimeout(m_networkTimeoutInMillis);
						// disable/enable Nagle's algorithm
						m_socket.setTcpNoDelay(this.m_addr.m_t4props.getTcpNoDelay());

						m_os = m_socket.getOutputStream();
						m_wbc = Channels.newChannel(m_os);
						m_is = m_socket.getInputStream();
						//m_sendBufSize = m_socket.getSendBufferSize();
						found = true;
						// Swastik: added code to start connection idle timers
						startConnectionIdleTimeout();
						// trace_connection - AM
						if (getT4props().isLogEnable(Level.FINER)) {
						    String temp = "found = " + found + ", Try [" + (numTry+1) + "] of 3, InetAddress [" + (i+1)
                                    + "] of " + m_addr.m_inetAddrs.length;
						    T4LoggingUtilities.log(getT4props(), Level.FINER, temp);
						}
						if (LOG.isDebugEnabled()) {
						    LOG.debug("found = {}, Try [{}] of 3, InetAddress [{}] of {}", found,
                                    (numTry + 1), (i + 1), m_addr.m_inetAddrs.length);
						}
					} catch (Exception e) {
						//
						// Make note of the exception, and keep trying all the
						// possible addresses.
						//
						// If no address works, we will chain all the exceptions
						// together,
						// and let the user figure out what went wrong.
						//
						eList.addElement(e);
						found = false;
					}
					i = i + 1;
				} // end while
				if (found == false) {
					try {
						// wait for 0.1 second before trying again
						Thread.sleep(100);
					} catch (Exception e) {
						// Do nothing.
					}
				}
				numTry = numTry + 1;
			} // end while
			if (found == false) {
				// trace_connection - AM
			    if (getT4props().isLogEnable(Level.FINER)) {
				String temp = "found = " + found + ", Try [" + (numTry+1) + "] of 3, InetAddress [" + i
                            + "] of " + m_addr.m_inetAddrs.length;
			        T4LoggingUtilities.log(getT4props(), Level.FINER, temp);
			    }
			    if (LOG.isDebugEnabled()) {
			        LOG.debug("found = {}, Try [{}] of 3, InetAddress [{}] of {}", found,
                            (numTry + 1), i, m_addr.m_inetAddrs.length);
			    }
				//
				// Couldn't open the socket
				//
				Exception eFirst = (Exception) eList.firstElement();

				//
				// Just add the first exception for now. I'd like to add the
				// entire list, but
				// it will take some thought to figure out the best way to put
				// each exception,
				// and it's associated exception cause (i.e. chained list)
				// together.
				// If there is more than one address, then we must be dealing
				// with a machine name.
				// Hopefully, the problem with the first address is
				// representitive of the problem
				// with all addresses.
				//
				SQLException se = TrafT4Messages.createSQLException(getT4props(), "socket_open_error", eFirst
						.getMessage() +", target url is ["+m_addr.recreateAddress()+"]");

				se.initCause(eFirst);
				throw se;
			}
            if (getT4props().isLogEnable(Level.FINER)) {
                String temp = "socket = " + m_socket;
                T4LoggingUtilities.log(getT4props(), Level.FINER, temp);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("socket = {}", m_socket);
            }
            InetSocketAddress isa = (InetSocketAddress) m_socket.getLocalSocketAddress();
            clientSocketAddress = isa.getAddress().getHostAddress();
            ic.setClientSocketAddress(clientSocketAddress);
		} // end if (p1.m_hSocket == null)

		//
		// If m_socket is not null, then we will assume it is already open.
		//
	} // end openIO

	synchronized LogicalByteArray doIO(short odbcAPI, LogicalByteArray buffer) throws SQLException {
		int cmpLength = 0;
		int totalLength = buffer.getLength();
		ByteBuffer dataBuffer = buffer.getDataBuffer();
		byte[] trailer = buffer.getTrailer();
		if (dataBuffer != null)
			totalLength += dataBuffer.limit();
		if (trailer != null)
			totalLength += trailer.length;

		if(totalLength  > 10000 && compress == Header.YES) //maybe totalLength - readHdrLength > 10000
		{
			compType = Header.COMP_DEFAULT;

			//dont set the databuffer
			dataBuffer = null;
			trailer = null;
		}
		else
		{
			cmpLength = 0;//totalLength - readHdrLength;
			compType = Header.COMP_0;
		}
		TrafT4Compression compression = null;
		if (compress == Header.YES ) {
			compression = new TrafT4CompressionZlib();
		}
		m_rheader.reuseHeader(odbcAPI, m_dialogueId);
		int buffer_index = 0;
		totalLength = buffer.getLength();
		int writeLen = totalLength;
		if (compression != null && compType == Header.COMP_DEFAULT) {
			try {
				compression.compress(compType, buffer, buffer_index, buffer.getLength());
				writeLen = buffer.getLocation();
				cmpLength = writeLen - Header.sizeOf();
			} catch (IOException e) {
				throw new SQLException(e);
			}
		}
        Header wheader = new Header(odbcAPI, (short)Vproc.jdbcMajorVersion,  m_dialogueId, totalLength - readHdrLength// minus the
                                                                                      // size of the
                                                                                      // Header
                , cmpLength, compress, compType, Header.WRITE_REQUEST_FIRST, Header.SIGNATURE,
                Header.CLIENT_HEADER_VERSION_BE, Header.PC, Header.TCPIP, Header.NO);

        if (getT4props().isLogEnable(Level.FINER)) {
            T4LoggingUtilities.log(getT4props(), Level.FINER, "Header before write : " + wheader.toString());
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Header before write : {}", wheader.toString());
        }
		// Send to the server
		//TCPIPDoWrite(wheader, buffer, buffer_index, buffer.getLength());
		TCPIPDoWrite(wheader, buffer, buffer_index, writeLen);

		if (dataBuffer != null && trailer != null) {
			TCPIPWriteByteBuffer(dataBuffer);
			TCPIPWriteByteBuffer(ByteBuffer.wrap(trailer));
		}

		// Receive from the server
		buffer.reset();

		// Read for READ_RESPONSE_FIRST
		int numRead = 0;
		int totalNumRead = 0;
		int whileCount1 = 0;
		long totalAvailable = 0;

		// Read the first part
		m_rheader.hdr_type_ = Header.READ_RESPONSE_FIRST;
		m_rheader.total_length_ = readHdrLength;

		// Keep reading until we have a header, but give up after 3 attempts.
		while (totalNumRead < readHdrLength && whileCount1 < 3) {
			numRead = TCPIPDoRead(m_rheader, buffer, totalNumRead);
			totalNumRead = totalNumRead + numRead;
			whileCount1 = whileCount1 + 1;
			// trace_connection - AM
            if (getT4props().isLogEnable(Level.FINER)) {
                String temp = "MessageBuffer read Try " + whileCount1 + " of 3, numRead=" + numRead
                        + ", totalNumRead=" + totalNumRead;
                T4LoggingUtilities.log(getT4props(), Level.FINER, temp);
            }
            if (LOG.isDebugEnabled()) {
                LOG.debug("MessageBuffer read Try {} of 3, numRead= {}, totalNumRead= {}",
                        whileCount1, numRead, totalNumRead);
            }
		} // end while

		// trace_connection - AM
		// if (totalNumRead < readHdrLength)
		if (numRead < readHdrLength) {
			//
			// We didn't even get the header back, so something is seriously
			// wrong.
			//
			SQLException se = TrafT4Messages.createSQLException(getT4props(), "problem_with_server_read");
			SQLException se2 = TrafT4Messages.createSQLException(getT4props(), "header_not_long_enough");

			se.setNextException(se2);
			throw se;
		}

		buffer.setLocation(0);
		m_rheader.extractFromByteArray(buffer);
        checkOdbcAPI(odbcAPI, m_rheader.operation_id_);
        if (m_rheader.operation_id_ == TRANSPORT.AS_API_GETOBJREF ||  m_rheader.operation_id_ == TRANSPORT.AS_API_ACTIVE_MASTER){
            ic.setDcsVersion(m_rheader.compatibilityVersion);
        }
        if (getT4props().isLogEnable(Level.FINER)) {
            T4LoggingUtilities
                .log(getT4props(), Level.FINER, "Header after read : " + m_rheader.toString());
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("Header after read : {}", m_rheader.toString());
        }
		if(odbcAPI == TRANSPORT.AS_API_GETOBJREF) {
			switch(m_rheader.version_) {
				case Header.CLIENT_HEADER_VERSION_BE:
				case Header.SERVER_HEADER_VERSION_BE:
					buffer.setByteSwap(false);
					break;
				case Header.SERVER_HEADER_VERSION_LE:
					buffer.setByteSwap(true);
					break;
				default:
					SQLException se = TrafT4Messages.createSQLException(getT4props(), "problem_with_server_read");
					SQLException se2 = TrafT4Messages.createSQLException(getT4props(), "wrong_header_version", String.valueOf(m_rheader.version_));

					se.setNextException(se2);
					throw se;
			}
		}

		if (m_rheader.signature_ != Header.SIGNATURE) {
			SQLException se = TrafT4Messages.createSQLException(m_t4props, "problem_with_server_read");
			SQLException se2 = TrafT4Messages.createSQLException(m_t4props, "wrong_header_signature", String
					.valueOf(Header.SIGNATURE), String.valueOf(m_rheader.signature_));

			se.setNextException(se2);
			throw se;
		}

		if (m_rheader.error_ != 0) {
			SQLException se = TrafT4Messages.createSQLException(m_t4props, "driver_err_error_from_server", String
					.valueOf(m_rheader.error_), String.valueOf(m_rheader.error_detail_));

			throw se;
		}
		if(m_rheader.compress_ind_ == Header.YES &&
				(0x00FF&m_rheader.compress_type_) == Header.COMP_DEFAULT) {
			totalAvailable = m_rheader.cmp_length_;
		}
		else {
			totalAvailable = m_rheader.total_length_;
		}

		numRead = 0;
		buffer.resize(m_rheader.total_length_ + readHdrLength); // make sure the
		// buffer is big
		// enough

		while (totalNumRead < (totalAvailable + readHdrLength)) {
			m_rheader.hdr_type_ = Header.READ_RESPONSE_NEXT;

			numRead = TCPIPDoRead(m_rheader, buffer, totalNumRead);
			totalNumRead = totalNumRead + numRead;
		}
		buffer.setLocation(totalNumRead);
		if (m_rheader.compress_ind_ == Header.YES &&
				(0x00FF&m_rheader.compress_type_) == Header.COMP_DEFAULT) {
            try {
                compression.uncompress(buffer);
            } catch (IOException e) {
                throw new SQLException(e);
            }
        }
		return buffer;
	} // end doIO

    private T4Properties getT4props() {
        return ic.getT4props();
    }

    private void checkOdbcAPI(short writeId, short readId) throws SQLException {
        if (getT4props().isLogEnable(Level.FINER)) {
            String tmp = "writeOdbcAPI = " + writeId + ", readOdbcAPI = " + readId;
            T4LoggingUtilities
                .log(getT4props(), Level.FINER, tmp);
        }
        if (LOG.isDebugEnabled()) {
            LOG.debug("writeOdbcAPI = {}, readOdbcAPI = {}", writeId, readId);
        }
		if (writeId != readId) {
            throw TrafT4Messages.createSQLException(getT4props(),"odbcAPI_is_not_consistent", writeId, readId);
        }
    }

	// ----------------------------------------------------------
	synchronized void closeIO() throws SQLException {

		try {

            if (m_socket == null) {
                if (getT4props().isLogEnable(Level.WARNING)) {
                    T4LoggingUtilities
                        .log(getT4props(), Level.WARNING, "m_socket_null_enter,return");
                }
                if (LOG.isWarnEnabled()) {
                    LOG.warn("m_socket_null_enter,return");
                }
                return;
            }

			if(m_t4props.isSSLEnabled()){
				m_socket.close();
				m_socket = null;
			} else {
				//m_socket.shutdownInput();
				m_socket.shutdownOutput();
				while(m_socket != null && !m_socket.isClosed()){
					try{
						if (m_socket.isInputShutdown() || m_socket.getInputStream().read() == -1) {
							m_socket.close();
							m_socket = null;
						}
					} catch (SocketTimeoutException ste) {
						m_socket.close();
						m_socket = null;
					}
				}
			}
		} catch (Exception e) {
			SQLException se = TrafT4Messages.createSQLException(m_t4props, "session_close_error", e.getMessage());
			se.initCause(e);
			throw se;
		} finally {
			closeTimers();
		}
	} // end closeIO

	void TCPIPWriteByteBuffer(ByteBuffer buffer) throws SQLException {

		if (m_socket == null) {
			SQLException se = TrafT4Messages.createSQLException(m_t4props, "socket_write_error");
			SQLException se2 = TrafT4Messages.createSQLException(m_t4props, "socket_is_closed_error");

			se.setNextException(se2);
			throw se;
		}

		try {
			m_wbc.write(buffer);
			m_os.flush();
		} catch (Exception e) {
			SQLException se = TrafT4Messages.createSQLException(m_t4props, "socket_write_error", e.getMessage());

			se.initCause(e);
			throw se;
		} finally {
			resetTimedOutConnection();
		}

	} // end TCPIPWriteByteBuffer

	// ----------------------------------------------------------
	void TCPIPDoWrite(Header hdr, LogicalByteArray buffer, int buffer_index, int bufcount) throws SQLException {
//		int error = 0;
//		short error_detail = 0;
//		int wcount = 0;
		int data_length = 0;

		if (m_socket == null) {
			SQLException se = TrafT4Messages.createSQLException(m_t4props, "socket_write_error");
			SQLException se2 = TrafT4Messages.createSQLException(m_t4props, "socket_is_closed_error");

			se.setNextException(se2);
			throw se;
		}

		switch (hdr.hdr_type_) {
		case Header.WRITE_REQUEST_FIRST:
		case Header.CLOSE_TCPIP_SESSION:
			buffer.setLocation(0);
			hdr.insertIntoByteArray(buffer);
		case Header.WRITE_REQUEST_NEXT:
			data_length = data_length + bufcount;

			send_nblk((int)hdr.operation_id_, buffer.getBuffer(), buffer_index, data_length);
			break;
		default:
			SQLException se = TrafT4Messages.createSQLException(m_t4props, "unknown_message_type_error");
			SQLException se2 = TrafT4Messages.createSQLException(m_t4props, "internal_error");
			SQLException se3 = TrafT4Messages.createSQLException(m_t4props, "cntact_traf_error");

			se.setNextException(se2);
			se2.setNextException(se3);
			throw se;
			// break;
		} // end switch (hdr.hdr_type)

	} // end TCPIPDoWrite

	// ----------------------------------------------------------
	int TCPIPDoRead(Header hdr, LogicalByteArray buffer, int buffer_index) throws SQLException {
		int numRead = 0;

		if (m_socket == null) {
			SQLException se = TrafT4Messages.createSQLException(m_t4props, "socket_read_error");
			SQLException se2 = TrafT4Messages.createSQLException(m_t4props, "socket_is_closed_error");

			se.setNextException(se2);
			throw se;
		}

		switch (hdr.hdr_type_) {
		case Header.READ_RESPONSE_FIRST:
		case Header.READ_RESPONSE_NEXT:
			numRead = recv_nblk((int)hdr.operation_id_, buffer.getBuffer(), buffer_index);
//			buffer.setLocation(numRead);
			break;
		default:
			SQLException se = TrafT4Messages.createSQLException(m_t4props, "unknown_message_type_error");
			SQLException se2 = TrafT4Messages.createSQLException(m_t4props, "internal_error");
			SQLException se3 = TrafT4Messages.createSQLException(m_t4props, "cntact_traf_error");

			se.setNextException(se2);
			se2.setNextException(se3);
			throw se;
		} // end switch (hdr.hdr_type)

		return numRead;

	} // end TCPIPDoRead

	// ----------------------------------------------------------
	void send_nblk(int srvrApi, byte[] buf, int offset, int len) throws SQLException {
		try {
			m_os.write(buf, offset, len);
			m_os.flush();
		} catch (Exception e) {
            if (srvrApi == TRANSPORT.SRVR_API_SQLCONNECT && e instanceof SocketException && e.getMessage().contains("socket write error")) {
                SQLException se = TrafT4Messages.createSQLException(m_t4props, "socket_write_error_2", e.getMessage());

                se.initCause(e);
                throw se;
            } else {
                SQLException se = TrafT4Messages.createSQLException(m_t4props, "socket_write_error", e.getMessage());

                se.initCause(e);
                throw se;
            }
		} finally {
			resetTimedOutConnection();
		}
	} // end send_nblk

    // ----------------------------------------------------------
    int recv_nblk(int srvrApi, byte[] buf, int offset) throws SQLException {
        int num_read = 0;
        int activeTime = 0;
        boolean cancelQueryAllowed = !(ic.getIgnoreCancel() || getT4props().getIgnoreCancel());
        int activeTimeBeforeCancel = ic.getActiveTimeBeforeCancel();
        boolean outerRetry = true;
        boolean cancelFlag = false;
        do {
            boolean innerRetry = true;
            int innerRetryCnt = 0;
            int pendingTimeout = m_timeout * 1000;
            if (pendingTimeout == 0){
                pendingTimeout = Integer.MAX_VALUE;
            }
            int waitCount = 0;
            do {
                try {
                    num_read = m_is.read(buf, offset, buf.length - offset);
                    // if the socket.read returns -1 then return 0 instead of -1
                    if (num_read < 0)
                        num_read = 0;
                    innerRetry = false;
                    outerRetry = false;
                } catch (SocketTimeoutException ste) {
                    if(!cancelFlag){
                        if (getT4props().isLogEnable(Level.FINER)) {
                            String temp = "Query timedout and try to cancel";
                            T4LoggingUtilities.log(getT4props(), Level.FINER, temp);
                        }
                        pendingTimeout -= m_networkTimeoutInMillis;
                        waitCount ++;
                        //timeout 10s function
                        switch (srvrApi){
	                        case TRANSPORT.AS_API_ACTIVE_MASTER:
	                        case TRANSPORT.AS_API_GETOBJREF:
		                        if(waitCount % 10 == 0){
			                        closeIO();
			                        throw TrafT4Messages.createSQLException(getT4props(), "connect_or_checkmaster_timeout", waitCount);
		                        }
		                        break;
	                        case TRANSPORT.AS_API_STOPSRVR:
		                        if(waitCount % (m_t4props.getCancelTimeOut()) == 0){
			                        closeIO();
			                        throw TrafT4Messages.createSQLException(getT4props(), "cancel_time_out", waitCount);
		                        }
		                        break;
	                        default:
		                        break;
                        }
                        //keep alive function
                        if (waitCount % 60 == 0) {
                            KeepAliveFun. putQueue(ic.getTrafT4Conn(), getT4props());
                        }
                        if (ic.isClosed()) {
                            try {
                                ic.getTrafT4Conn().close();
                            } catch (SQLException e) {
                                LOG.warn("connection close failed");
                            }
                            throw TrafT4Messages.createSQLException(getT4props(), "jdbc_keep_alive",
                                ic.getRemoteProcess());
                        }
                        if (pendingTimeout <= 0) {
                            innerRetry = false;
                            break;
                        }
                    }
                } catch (IOException ioe) {
                    if (innerRetryCnt <= 3) {
                        try {
                            innerRetryCnt++;
                            Thread.sleep(10);
                        } catch (InterruptedException ie) {
                        }
                    } else {
                        SQLException se = TrafT4Messages.createSQLException(getT4props(), "problem_with_server_read");
                        se.setNextException(new SQLException(ioe));
                        if (getT4props().isLogEnable(Level.FINER)) {
                            String temp = "Socket.read returned an exception " + se.toString();
                            T4LoggingUtilities.log(getT4props(), Level.FINER, temp);
                        }
                        throw se;
                    }
                }
            } while (innerRetry);
            if (!outerRetry)
                break;
            // Connection or connection related requests timed out
            if (srvrApi == TRANSPORT.SRVR_API_SQLCONNECT || srvrApi == TRANSPORT.AS_API_GETOBJREF) {
                closeIO();
                SQLException se = TrafT4Messages.createSQLException(m_t4props,
                        "connection timed out in [" + m_timeout + "] seconds");
                if (getT4props().isLogEnable(Level.FINER)) {
                    String temp = "Socket.read timed out in [" + m_timeout + "] seconds, networkTimeoutInMillis "
                            + m_networkTimeoutInMillis;
                    T4LoggingUtilities.log(getT4props(), Level.FINER, temp);
                }
                throw se;
            }
	        if (activeTimeBeforeCancel != -1) {
		        if (m_timeout != 0) {
			        activeTime += m_timeout;
		        } else {
			        activeTime += (m_networkTimeoutInMillis / 1000);
		        }
		        if (activeTime >= activeTimeBeforeCancel) {
			        try {
				        ic.cancel(-1);
			        } catch (Exception e) {
				        //cancel error
				        if (getT4props().isLogEnable(Level.FINER)) {
					        T4LoggingUtilities
							        .log(getT4props(), Level.FINER, "ENTRY", e.getMessage());
				        }
				        if (LOG.isDebugEnabled()) {
					        LOG.debug("ENTRY. <{}>", e.getMessage());
				        }
			        }
		        }
	        }
        } while (outerRetry);
        return num_read;
    } // recv_nblk

	/** ***************************************** */
	// Start of connectino timeout related code //
	/** ***************************************** */
	void closeTimers() {
		if (m_connectionIdleTimeout > 0) {
			T4TimerThread t = T4TimerThread.getThread(m_dialogueId);
			if (t != null) {
				// t.interrupt(); //SB:2/24/05
				T4TimerThread.removeThread(this.m_dialogueId);
			}
		}
	}

	void resetConnectionIdleTimeout() {
		if (m_connectionIdleTimeout > 0) {
			T4TimerThread t = T4TimerThread.getThread(m_dialogueId);
			if (t != null) {
				t.reset(m_connectionIdleTimeout * 1000);
			} else { // first time
				startConnectionIdleTimeout();
			}
		}
	}

	private void resetTimedOutConnection() {
		if (m_connectionIdleTimeout > 0) {
			// check connection idle timeout
			boolean timedOut = checkConnectionIdleTimeout();
			if (timedOut) {
				startConnectionIdleTimeout(); // this closes existing timers
				// and starts a new one
				// required for long runnign queries
			} else {
				resetConnectionIdleTimeout();
			}
		}
	}

	boolean checkConnectionIdleTimeout() {
		if (m_connectionIdleTimeout > 0) {
			T4TimerThread t = T4TimerThread.getThread(m_dialogueId);
			if (t != null && t.getTimedOut()) {
				return true;
			}
		}
		return false;
	}

	void startConnectionIdleTimeout() {
		if (m_connectionIdleTimeout > 0 && m_dialogueId > 0) {
			closeTimers();
			T4TimerThread m_t4TimerThread = new T4TimerThread(m_dialogueId, m_connectionIdleTimeout * 1000);
			// m_t4TimerThread.start(); ==> // SB:2/24/05 this class is no
			// longer
			// inheriting the thread package
			// However it can be modified if
			// need be - see class comments.
		}
	}

	String getClientSocketAddress(){
		return clientSocketAddress;
	}
} // end class InputOutput
