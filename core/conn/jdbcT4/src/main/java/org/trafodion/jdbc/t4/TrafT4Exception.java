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

import java.sql.SQLException;

class TrafT4Exception extends SQLException {
	protected String messageId;

	private String reason;
	private String sqlSate;
	private int vendorCode;

	public TrafT4Exception(String reason, String SQLState, int vendorCode, String msgId) {
		super(reason, SQLState, vendorCode);
		if (msgId == null) {
			messageId = "";
		} else {
			messageId = msgId;
		}
		this.reason = reason;
		this.sqlSate = SQLState;
		this.vendorCode = vendorCode;
	}
	public static TrafT4Exception getNewTrafT4Exception(TrafT4Exception trafT4Exception,String message){
		TrafT4Exception newTrafT4Exception = new TrafT4Exception(
			getNewErrorMessage(trafT4Exception.getReason(),message),
			trafT4Exception.getSQLState(),
			trafT4Exception.getVendorCode(),
			null);
		return newTrafT4Exception;
	}

	/**
	 * @Description: This method is used to get a new error description
	 * @Param:  String oldMessage String errorMessage
	 * @return:  String newErrorMessage
	 */
	private static String getNewErrorMessage(String oldMessage,String errorMessage){
		if(oldMessage == null || oldMessage == ""){
			return errorMessage;
		}else {
			StringBuffer newErrorMessage = new StringBuffer(oldMessage);
			int frontIndex = oldMessage.indexOf("[");
			int lastIndex = oldMessage.lastIndexOf("[");
			if(frontIndex == -1 || lastIndex == -1){
				return newErrorMessage.append(errorMessage).toString();
			}else {
				if(frontIndex == lastIndex){
					return newErrorMessage.append(errorMessage).toString();
				}else {
					newErrorMessage.insert(lastIndex,errorMessage);
					return newErrorMessage.toString();
				}
			}
		}
	}

	public String getReason() {
		return reason;
	}

	public String getSqlSate() {
		return sqlSate;
	}

	public int getVendorCode() {
		return vendorCode;
	}

} // end class TrafT4Exception
