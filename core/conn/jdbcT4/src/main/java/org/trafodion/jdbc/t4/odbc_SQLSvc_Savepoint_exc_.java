//@@@ START COPYRIGHT @@@
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
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
class odbc_SQLSvc_Savepoint_exc_ {
    int exception_nr;
    int exception_detail;
    String ParamError;
    ERROR_DESC_LIST_def SQLError;


    // From odbc_cl.h
    //
    // struct odbc_SQLSvc_Savepoint_exc_ {
    // size_int exception_nr;
    // size_int exception_detail;
    // union {
    // odbc_SQLSvc_ParamError ParamError;
    // odbc_SQLSvc_SQLError SQLError;
    // } u;
    // };

    String clientErrorText = null;

    // Exception number constants for
    // operation 'odbc_SQLSvc_Savepoint'
    static final int odbc_SQLSvc_Savepoint_ParamError_exn_ = 1;
    static final int odbc_SQLSvc_Savepoint_InvalidConnection_exn_ = 2;
    static final int odbc_SQLSvc_Savepoint_SQLError_exn_ = 3;
    static final int odbc_SQLSvc_Savepoint_SQLInvalidHandle_exn_ = 4;
    static final int odbc_SQLSvc_Savepoint_SavepointError_exn_ = 5;
    
    private final static org.slf4j.Logger LOG =
            LoggerFactory.getLogger(odbc_SQLSvc_Savepoint_exc_.class);

    void extractFromByteArray(LogicalByteArray buffer1, String addr, InterfaceConnection ic)
        throws CharacterCodingException, UnsupportedCharsetException, SQLException {
        exception_nr = buffer1.extractInt();
        exception_detail = buffer1.extractInt();
        String temp0 = Integer.toString(exception_nr);
        String temp1 = Integer.toString(exception_detail);
        switch (exception_nr) {
            case TRANSPORT.CEE_SUCCESS:
                break;
            case odbc_SQLSvc_Savepoint_ParamError_exn_:
                if (ic.getT4props().isLogEnable(Level.FINER)) {
                    String temp = "odbc_SQLSvc_Savepoint_ParamError_exn_ :";
                    T4LoggingUtilities.log(ic.getT4props(), Level.FINER, temp, addr);
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("odbc_SQLSvc_Savepoint_ParamError_exn_ : {}", addr);
                }
                ParamError = ic.decodeBytes(buffer1.extractString(), 1);
                throw TrafT4Messages.createSQLException(null, "ids_program_error", ParamError,
                        addr);
            case odbc_SQLSvc_Savepoint_SQLError_exn_:
                SQLError = new ERROR_DESC_LIST_def();
                SQLError.extractFromByteArray(buffer1, ic);
                break;
            case odbc_SQLSvc_Savepoint_InvalidConnection_exn_:
                if (ic.getT4props().isLogEnable(Level.FINER)) {
                    String temp = "odbc_SQLSvc_Savepoint_InvalidConnection_exn_ :";
                    T4LoggingUtilities.log(ic.getT4props(), Level.FINER, temp, addr);
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("odbc_SQLSvc_Savepoint_InvalidConnection_exn_ : {}", addr);
                }
                throw TrafT4Messages.createSQLException(null, "ids_08_s01");
            case odbc_SQLSvc_Savepoint_SQLInvalidHandle_exn_:
                if (ic.getT4props().isLogEnable(Level.FINER)) {
                    String temp = "odbc_SQLSvc_Savepoint_SQLInvalidHandle_exn_ :";
                    T4LoggingUtilities.log(ic.getT4props(), Level.FINER, temp, addr);
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("odbc_SQLSvc_Savepoint_SQLInvalidHandle_exn_ : {}", addr);
                }
                throw TrafT4Messages.createSQLException(null, "ids_transaction_error", temp1, addr);
            default:
                if (ic.getT4props().isLogEnable(Level.FINER)) {
                    String temp = "ids_unknown_reply_error :";
                    T4LoggingUtilities.log(ic.getT4props(), Level.FINER, temp, addr);
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("ids_unknown_reply_error : {}", addr);
                }
                throw TrafT4Messages.createSQLException(null, "ids_unknown_reply_error", temp0,
                        temp1);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Exception number = ").append(exception_nr);
        sb.append(", exception detail = ").append(exception_detail);
        if (ParamError != null) {
            sb.append(", param error = ").append(ParamError);
        }
        if (clientErrorText != null) {
            sb.append(", clinet error text = ").append(clientErrorText);
        }
        return sb.toString();
    }
}
