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

class OUT_CONNECTION_CONTEXT_def {
	static final long OUTCONTEXT_OPT1_ENFORCE_ISO88591 = 1; // (2^0)
	static final long OUTCONTEXT_OPT1_IGNORE_SQLCANCEL = 1073741824; // (2^30)
	static final long OUTCONTEXT_OPT1_EXTRA_OPTIONS = 2147483648L; // (2^31)
	static final long OUTCONTEXT_OPT1_DOWNLOAD_CERTIFICATE = 536870912; //(2^29)

	static final long OUTCONTEXT_OPT2_ENFORCE_ISUSECACHE = 1; // (2^0)

	private final static org.slf4j.Logger LOG =
            LoggerFactory.getLogger(OUT_CONNECTION_CONTEXT_def.class);

	VERSION_LIST_def versionList;

	short nodeId;
	int processId;

	String computerName;
	String catalog;
    private String schema;

	int optionFlags1;
	int optionFlags2;

	String _roleName;
	boolean _enforceISO;
	boolean _ignoreCancel;
	private boolean _isUseCache = false;

	byte [] certificate;

	void extractFromByteArray(LogicalByteArray buf, InterfaceConnection ic) throws SQLException,
			UnsupportedCharsetException, CharacterCodingException {
		versionList = new VERSION_LIST_def();
		versionList.extractFromByteArray(buf);

		nodeId = buf.extractShort();
		processId = buf.extractInt();
		computerName = ic.decodeBytes(buf.extractString(), 1);

		catalog = ic.decodeBytes(buf.extractString(), InterfaceUtilities.SQLCHARSETCODE_UTF8);
		schema = ic.decodeBytes(buf.extractString(), InterfaceUtilities.SQLCHARSETCODE_UTF8);

		optionFlags1 = buf.extractInt();
		optionFlags2 = buf.extractInt();
		this._isUseCache = (optionFlags2 & OUTCONTEXT_OPT2_ENFORCE_ISUSECACHE) > 0;

		this._enforceISO = (optionFlags1 & OUTCONTEXT_OPT1_ENFORCE_ISO88591) > 0;
		this._ignoreCancel = (optionFlags1 & OUTCONTEXT_OPT1_IGNORE_SQLCANCEL) > 0;
		if((optionFlags1 & OUTCONTEXT_OPT1_DOWNLOAD_CERTIFICATE) > 0) {
			certificate = buf.extractByteArray();
		}
		else if ((optionFlags1 & OUTCONTEXT_OPT1_EXTRA_OPTIONS) > 0) {
			try {
				this.decodeExtraOptions(ic.decodeBytes(buf.extractString(), InterfaceUtilities.SQLCHARSETCODE_UTF8));
			} catch (Exception e) {
                if (ic.getT4props().isLogEnable(Level.FINER)) {
                    T4LoggingUtilities.log(ic.getT4props(), Level.FINER,
                            "An error occurred parsing OutConnectionContext: " + e.getMessage());
                }
                if (LOG.isDebugEnabled()) {
                    LOG.debug("An error occurred parsing OutConnectionContext: {}", e.getMessage());
                }
			}
		}
	}

	public void decodeExtraOptions(String options) {
		String[] opts = options.split(";");
		String token;
		String value;
		int index;

		for (int i = 0; i < opts.length; i++) {
			index = opts[i].indexOf('=');
			token = opts[i].substring(0, index).toUpperCase();
			value = opts[i].substring(index + 1);

			if (token.equals("RN")) {
				this._roleName = value;
			}
		}
	}

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    public boolean isUseCache(){
		return this._isUseCache;
    }
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("VersionList = ").append(versionList.toString());
        sb.append(", nodeId = ").append(nodeId);
        sb.append(", processId = ").append(processId);
        sb.append(", computerName = ").append(computerName);
        sb.append(", catalog = ").append(catalog);
        sb.append(", schema = ").append(schema);
        sb.append(", optionFlags1 = ").append(optionFlags1);
        sb.append(", optionFlags2 = ").append(optionFlags2);
        sb.append(", _enforceISO = ").append(_enforceISO);
        sb.append(", _ignoreCancel = ").append(_ignoreCancel);
        if (_roleName != null) {
            sb.append(", _roleName = ").append(_roleName);
        }
        sb.append("_isUseCache = ").append(_isUseCache);
        return sb.toString();
    }
}
