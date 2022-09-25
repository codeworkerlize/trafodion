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

class odbc_Dcs_GetObjRefHdl_exc_ {
	int exception_nr;
	int exception_detail;

	String ErrorText;
	int errorCode;

	//
	// It looks like ODBC doesn't generate error text in all
	// error cases, so the following variable will contain
	// any error text generated by this JDBC driver.
	// Note, this variable is not part of the message, but it
	// represents a value stored in the TrafT4Messages_*.properties file.
	//

	String clientErrorText;

	static final int odbc_Dcs_GetObjRefHdl_ASParamError_exn_ = 1;
	static final int odbc_Dcs_GetObjRefHdl_ASTimeout_exn_ = 2;
	static final int odbc_Dcs_GetObjRefHdl_ASNoSrvrHdl_exn_ = 3;
	static final int odbc_Dcs_GetObjRefHdl_ASTryAgain_exn_ = 4;
	static final int odbc_Dcs_GetObjRefHdl_ASNotAvailable_exn_ = 5;
	static final int odbc_Dcs_GetObjRefHdl_DSNotAvailable_exn_ = 6;
	static final int odbc_Dcs_GetObjRefHdl_PortNotAvailable_exn_ = 7;
	static final int odbc_Dcs_GetObjRefHdl_InvalidUser_exn_ = 8;
	static final int odbc_Dcs_GetObjRefHdl_LogonUserFailure_exn_ = 9;
	static final int odbc_Dcs_GetObjRefHdl_InvalidTenant_exn_ = 10;
	static final int odbc_Dcs_GetObjRefHdl_SessionLimit_exn_ = 11;
	static final int odbc_Dcs_GetObjRefHdl_TenantSessionLimit_exn_ = 12;
	static final int odbc_Dcs_GetObjRefHdl_Ipwhitelist_exn = 13;
	static final int odbc_Dcs_GetObjRefHdl_DistributionSevr_exn = 14;

	// -------------------------------------------------------------------
	void extractFromByteArray(LogicalByteArray buffer1, InterfaceConnection ic) throws SQLException,
			UnsupportedCharsetException, CharacterCodingException {
		exception_nr = buffer1.extractInt();
		exception_detail = buffer1.extractInt();

//		String temp0 = Integer.toString(exception_nr);
//		String temp1 = Integer.toString(exception_detail);

		ErrorText = ic.decodeBytes(buffer1.extractString(), 1);

		switch (exception_nr) {
		case TRANSPORT.CEE_SUCCESS:
			break;
		case odbc_Dcs_GetObjRefHdl_ASParamError_exn_:
			throw TrafT4Messages.createSQLException(ic.getT4props(), "ids_program_error", ErrorText);
		case odbc_Dcs_GetObjRefHdl_LogonUserFailure_exn_:
			throw TrafT4Messages.createSQLException(ic.getT4props(), "ids_unable_to_logon", "");
		case odbc_Dcs_GetObjRefHdl_ASNotAvailable_exn_:
			throw TrafT4Messages.createSQLException(ic.getT4props(), "ids_dcs_srvr_not_available", ErrorText);
		case odbc_Dcs_GetObjRefHdl_DSNotAvailable_exn_:
			throw TrafT4Messages.createSQLException(ic.getT4props(), "ids_ds_not_available", ic.getT4props()
					.getServerDataSource());
		case odbc_Dcs_GetObjRefHdl_InvalidTenant_exn_:
			throw TrafT4Messages.createSQLException(ic.getT4props(), "tenant_invalid_error");
		case odbc_Dcs_GetObjRefHdl_SessionLimit_exn_:
		    throw TrafT4Messages.createSQLException(ic.getT4props(), "session_limit");
		case odbc_Dcs_GetObjRefHdl_TenantSessionLimit_exn_:
		    throw TrafT4Messages.createSQLException(ic.getT4props(), "tenant_session_limit");
		case odbc_Dcs_GetObjRefHdl_PortNotAvailable_exn_:
		case odbc_Dcs_GetObjRefHdl_ASTryAgain_exn_:
		case odbc_Dcs_GetObjRefHdl_ASNoSrvrHdl_exn_:
		case -27:
		case -29:

			// should be retried by the driver so dont throw exception
			clientErrorText = "ids_port_not_available";
			break;
		case odbc_Dcs_GetObjRefHdl_InvalidUser_exn_:
			throw TrafT4Messages.createSQLException(ic.getT4props(), "ids_28_000");
		case odbc_Dcs_GetObjRefHdl_ASTimeout_exn_:
			throw TrafT4Messages.createSQLException(ic.getT4props(), "ids_s1_t00", ic.getTargetServer(), ic.getLoginTimeout());
		case odbc_Dcs_GetObjRefHdl_Ipwhitelist_exn:
			throw TrafT4Messages.createSQLException(ic.getT4props(), ErrorText);
		case odbc_Dcs_GetObjRefHdl_DistributionSevr_exn:
			throw TrafT4Messages.createSQLException(ic.getT4props(), "distribution_server_error", ErrorText);
		default:
			throw TrafT4Messages.createSQLException(ic.getT4props(), "unknown_connect_error");
		}
	}

	@Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Exception number = ").append(exception_nr);
        sb.append(", exception detail = ").append(exception_detail);
        sb.append(", error code = ").append(errorCode);
        if (ErrorText != null) {
            sb.append(", error text = ").append(ErrorText);
        }
        if (clientErrorText != null) {
            sb.append(", clinet error text = ").append(clientErrorText);
        }
        return sb.toString();
    }
}
