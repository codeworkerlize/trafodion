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
import java.util.ArrayList;
import java.util.List;
import java.net.InetAddress;
import java.net.UnknownHostException;

class ConnectMessage {

    static LogicalByteArray marshal(CONNECTION_CONTEXT_def inContext, USER_DESC_def userDesc, int srvrType,
			short retryCount, int optionFlags1, int optionFlags2, String vproc, InterfaceConnection ic)
			throws CharacterCodingException, UnsupportedCharsetException {
		int wlength = Header.sizeOf();
		LogicalByteArray buf = null;
		
		String ccExtention = "";
		byte[] ccExtentionBytes = null;
		
        String sessionName = ic.getSessionName();
        if (sessionName == null) sessionName = "";
        
        InetAddress ip;
        String clientIpAddress = ic.getClientSocketAddress();;
        String clientHostName;

        try {
            ip = InetAddress.getLocalHost();
            clientHostName = ip.getHostName();
 
        } catch (UnknownHostException e) {
            clientHostName = "";
        }
        String userName = ic.getUid();
        String roleName = inContext.userRole;
        String tenantName = inContext.tenantName;
        String applicationName = ic.getApplicationName();

        String ipMapping = ic.getT4props().getIpMapping();
        String specifiedServer = ic.getT4props().getSpecifiedServer();

        StringBuffer format = new StringBuffer("{");
        List<String> param = new ArrayList<String>();

        checkAndAddParams("sessionName", sessionName, format, param);
        checkAndAddParams("clientIpAddress", clientIpAddress, format, param);
        checkAndAddParams("clientHostName", clientHostName, format, param);
        checkAndAddParams("userName", userName, format, param);
        checkAndAddParams("roleName", roleName, format, param);
        checkAndAddParams("applicationName", applicationName, format, param);
        checkAndAddParams("tenantName", tenantName, format, param);
        checkAndAddParams("ipMapping", ipMapping, format, param);
        checkAndAddParams("specifiedServer", specifiedServer, format, param);
        // remove the last comma
        format = format.delete(format.length() - 1, format.length()).append("}");
        ccExtention = String.format(format.toString(), param.toArray());

        ccExtentionBytes = ic.encodeString(ccExtention, InterfaceUtilities.SQLCHARSETCODE_UTF8);

		byte[] vprocBytes = ic.encodeString(vproc, 1);
		byte[] clientUserBytes = ic.encodeString(userName, 1);
		
		wlength += inContext.sizeOf(ic);
		wlength += userDesc.sizeOf(ic);

		wlength += TRANSPORT.size_int; // srvrType
		wlength += TRANSPORT.size_short; // retryCount
		wlength += TRANSPORT.size_int; // optionFlags1
		wlength += TRANSPORT.size_int; // optionFlags2
		wlength += TRANSPORT.size_bytes(vprocBytes);
		wlength += ccExtentionBytes.length;

		buf = new LogicalByteArray(wlength, Header.sizeOf(), ic.getByteSwap());

		inContext.insertIntoByteArray(buf);
		userDesc.insertIntoByteArray(buf);

		buf.insertInt(srvrType);
		buf.insertShort(retryCount);
		buf.insertInt(optionFlags1);
		buf.insertInt(optionFlags2);
		buf.insertString(vprocBytes);
			
		//TODO: restructure all the flags and this new param
		buf.insertString(clientUserBytes);
		buf.insertString(ccExtentionBytes);

		return buf;
	}

    private static void checkAndAddParams(String keyName, String value, StringBuffer format, List<String> param) {
        if (value != null) {
            format.append("\"").append(keyName).append("\":\"%s\",");
            param.add(value);
        }
    }
}
