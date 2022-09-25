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
import org.trafodion.jdbc.t4.tmpl.*;

class T4DcsConnection {

    private final static org.slf4j.Logger LOG = LoggerFactory.getLogger(T4DcsConnection.class);
    private InputOutput io;
    private InterfaceConnection ic;
    private T4Address addr;

    public T4DcsConnection(InterfaceConnection ic) throws SQLException {
        this.ic = ic;
        this.addr = new T4Address(ic.getT4props(), ic.getT4props().getUrl());

        io = new InputOutput(ic.getT4props(), addr);
        io.setInterfaceConnection(ic);
        io.setNetworkTimeoutInMillis(ic.getT4props().getNetworkTimeoutInMillis());
        io.setConnectionIdleTimeout(ic.getConnectionTimeout());
    }

    public T4DcsConnection(InterfaceConnection ic,String addresse) throws SQLException {
        this.ic = ic;
        this.addr = new T4Address(ic.getT4props(), addresse);

        io = new InputOutput(ic.getT4props(), addr);
        io.setInterfaceConnection(ic);
        io.setNetworkTimeoutInMillis(ic.getT4props().getNetworkTimeoutInMillis());
        io.setConnectionIdleTimeout(ic.getConnectionTimeout());
    }

    protected T4Properties getT4props() {
        return ic.getT4props();
    }

    /**
     * This method will establish an initial connection to the ODBC association
     * server.
     * @param srvrType
     * @param retryCount The number of times to retry the connection
     * @return A ConnectReply class representing the reply from the association
     *         server is returned
     * @throws SQLException
     */
    ConnectReply getConnection(int srvrType, short retryCount) throws SQLException {
        if (getT4props().isLogEnable(Level.FINER)) {
            T4LoggingUtilities.log(getT4props(), Level.FINER, "ENTRY");
        }
        if(LOG.isDebugEnabled()){
            LOG.debug("ENTRY");
        }

        if (ic.getInContext() == null || ic.getUserDescription() == null) {
            SQLException se = TrafT4Messages.createSQLException(getT4props(), "internal_error");
            SQLException se2 = TrafT4Messages.createSQLException(getT4props(), "contact_traf_error");

            se.setNextException(se2);
            throw se;
        }
        try {
            io.setTimeout(ic.getLoginTimeout());
            io.openIO();
            ic.getTrafT4Conn().checkLoginTimeout(getT4props());
            // Do marshaling of input parameters.
            LogicalByteArray wbuffer = ConnectMessage.marshal(ic.getInContext(), ic.getUserDescription(), srvrType,
                    retryCount, T4Connection.INCONTEXT_OPT1_CLIENT_USERNAME, 0, Vproc.getVproc(), ic);

            // Send message to the ODBC Association server.
            LogicalByteArray rbuffer = io.doIO(TRANSPORT.AS_API_GETOBJREF, wbuffer);

            // Process output parameters
            ConnectReply cr = new ConnectReply(rbuffer, ic);

            // Close IO
            io.setTimeout(getT4props().getLoginTimeout());

            String name = null;
            if (addr.m_ipAddress != null) {
                name = addr.m_ipAddress;
            } else if (addr.m_machineName != null) {
                name = addr.m_machineName;

            }
            cr.fixupSrvrObjRef(ic, name);

            return cr;
        } catch (SQLException se) {
            throw se;
        } catch (CharacterCodingException e) {
            SQLException se = TrafT4Messages.createSQLException(getT4props(), "translation_of_parameter_failed",
                    "ConnectMessage", e.getMessage());
            se.initCause(e);
            throw se;
        } catch (UnsupportedCharsetException e) {
            SQLException se =
                    TrafT4Messages.createSQLException(getT4props(), "unsupported_encoding", e.getCharsetName());
            se.initCause(e);
            throw se;
        } catch (Exception e) {
            SQLException se =
                    TrafT4Messages.createSQLException(getT4props(), "as_connect_message_error", e.getMessage());
            se.initCause(e);
            throw se;
        } finally {
            if (io != null)
                io.closeIO();
        }
    }

    /**
     * This method will establish an initial connection to the ODBC association
     * server.
     * @param srvrType
     * @param srvrObjRef
     * @param stopType
     * @return A CancelReply class representing the reply from the association
     *         server is returned
     * @throws SQLException
     */
    CancelReply cancel(int srvrType, String srvrObjRef, int stopType) throws SQLException {
        if (getT4props().isLogEnable(Level.FINER)) {
            T4LoggingUtilities.log(getT4props(), Level.FINER, "ENTRY");
        }
        if(LOG.isDebugEnabled()){
            LOG.debug("ENTRY");
        }

        try {
            //
            // Do marshaling of input parameters.
            //
            LogicalByteArray wbuffer = CancelMessage.marshal(ic.getDialogueId(), srvrType, srvrObjRef, stopType, ic);

            io.openIO();
            io.setTimeout(getT4props().getNetworkTimeout());

            //
            // Send message to the ODBC Association server.
            //
            LogicalByteArray rbuffer = io.doIO(TRANSPORT.AS_API_STOPSRVR, wbuffer);

            //
            // Process output parameters
            //
            CancelReply cr = new CancelReply(rbuffer, ic);

            //
            // Close IO
            //
            // io1.setTimeout(getT4props().getCloseConnectionTimeout());
            io.setTimeout(getT4props().getNetworkTimeout());

            if (getT4props().isLogEnable(Level.FINER)) {
                T4LoggingUtilities.log(getT4props(), Level.FINER, "LEAVE");
            }
            if(LOG.isDebugEnabled()){
                LOG.debug("LEAVE");
            }

            return cr;
        } catch (SQLException se) {
            if (se.getErrorCode() == -29231) {
                throw se;
            }
            SQLException se2 = TrafT4Messages.createSQLException(getT4props(), "dcs_cancel_message_error", se.getMessage());
            throw se2;
        } catch (Exception e) {
            SQLException se = TrafT4Messages.createSQLException(getT4props(), "as_cancel_message_error", e.getMessage());
            se.initCause(e);
            throw se;
        } finally {
            if (io != null)
                io.closeIO();
        }
    }

    KeepAliveCheckReply keepAliveCheck(String dialogueIds, String remoteprocess) throws SQLException {
        if (getT4props().isLogEnable(Level.FINER)) {
            T4LoggingUtilities.log(getT4props(), Level.FINER, "ENTRY");
        }
        if(LOG.isDebugEnabled()){
            LOG.debug("ENTRY");
        }

        try {
            // Do marshaling of input parameters.
            LogicalByteArray wbuffer = KeepAliveCheckMessage
                .marshal(dialogueIds, remoteprocess, ic);

            io.openIO();
            io.setTimeout(getT4props().getNetworkTimeout());

            // Send message to the ODBC Association server.
            LogicalByteArray rbuffer = io.doIO(TRANSPORT.AS_API_DCSMXOKEEPALIVE, wbuffer);

            // Process output parameters
            KeepAliveCheckReply kcr = new KeepAliveCheckReply(rbuffer, ic);

            if (getT4props().isLogEnable(Level.FINER)) {
                T4LoggingUtilities.log(getT4props(), Level.FINER, "LEAVE");
            }
            if(LOG.isDebugEnabled()){
                LOG.debug("LEAVE");
            }
            // Close IO
            return kcr;
        } catch (SQLException se) {
            SQLException se2 = TrafT4Messages.createSQLException(getT4props(), "keep_alive_sql_message_error", se.getMessage());
            throw se2;
        } catch (Exception e) {
            SQLException se = TrafT4Messages.createSQLException(getT4props(), "keep_alive_message_error", e.getMessage());
            se.initCause(e);
            throw se;
        } finally {
            if (io != null)
                io.closeIO();
        }
    }

    /**
     * check active Dcsmaster
     * @return A CheckActiveMasterReply class representing the reply from the association
     *         server is returned
     * @throws SQLException
     */
    CheckActiveMasterReply checkActiveMaster() throws Exception {
        if (getT4props().isLogEnable(Level.FINER)) {
            T4LoggingUtilities.log(getT4props(), Level.FINER, "ENTRY");
        }
        if(LOG.isDebugEnabled()){
            LOG.debug("ENTRY");
        }

        try {
            LogicalByteArray wbuffer = CheckActiveMasterMessage.marshal(ic);

            io.openIO();
            io.setTimeout(getT4props().getNetworkTimeout());

            LogicalByteArray rbuffer = io.doIO(TRANSPORT.AS_API_ACTIVE_MASTER, wbuffer);

            CheckActiveMasterReply camr = new CheckActiveMasterReply(rbuffer, ic);

            if (getT4props().isLogEnable(Level.FINER)) {
                T4LoggingUtilities.log(getT4props(), Level.FINER, "LEAVE");
            }
            if(LOG.isDebugEnabled()){
                LOG.debug("LEAVE");
            }

            return camr;
        } catch (Exception e) {
            throw e;
        } finally {
            if (io != null)
                io.closeIO();
        }
    }

}
