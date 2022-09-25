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

public class SQLWarningOrError {
	int rowId;
	int sqlCode;
	String text;
	String sqlState;
    boolean isWarning = false;
	public SQLWarningOrError(int rowId, int sqlCode, String text, String sqlState) {
		this.rowId = rowId;
		this.sqlCode = sqlCode;
		this.text = text;
		this.sqlState = sqlState;
	}

	public SQLWarningOrError(LogicalByteArray buf, InterfaceConnection ic, int charset)
			throws CharacterCodingException, UnsupportedCharsetException {
		rowId = buf.extractInt();
		sqlCode = buf.extractInt();
		text = ic.decodeBytes(buf.extractString(), charset);
		sqlState = new String(buf.extractByteArray(5));
		buf.extractByte(); // null terminator

		if (text.indexOf("WARNING") >= 0){
			isWarning = true;
		}
	}
}
