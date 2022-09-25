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

class InitializeDialogueMessage {
	static LogicalByteArray marshal(USER_DESC_def userDesc, CONNECTION_CONTEXT_def inContext, int dialogueId,
			int optionFlags1, int optionFlags2, String sessionID, InterfaceConnection ic)
			throws CharacterCodingException, UnsupportedCharsetException {
		int wlength = Header.sizeOf();
		LogicalByteArray buf;

		byte[] sessionBytes = ic.encodeString(sessionID, InterfaceUtilities.SQLCHARSETCODE_UTF8);
		byte[] clientUserBytes = ic.encodeString(System.getProperty("user.name"), InterfaceUtilities.SQLCHARSETCODE_UTF8);

		wlength += userDesc.sizeOf(ic);
		wlength += inContext.sizeOf(ic);

		wlength += TRANSPORT.size_int; // dialogueId
		wlength += TRANSPORT.size_int; // optionFlags1
		wlength += TRANSPORT.size_int; // optionFlags2
		wlength += sessionBytes.length;
		wlength += clientUserBytes.length;

        int xattrslen = getXAttrsLength(ic);
        wlength += xattrslen;

		buf = new LogicalByteArray(wlength, Header.sizeOf(), ic.getByteSwap());


		userDesc.insertIntoByteArray(buf);
		inContext.insertIntoByteArray(buf);
		buf.insertInt(dialogueId);
		buf.insertInt(optionFlags1);
		buf.insertInt(optionFlags2);

		if((optionFlags1 & T4Connection.INCONTEXT_OPT1_SESSIONNAME) != 0) 
		{
			buf.insertString(sessionBytes);
		}
		if((optionFlags1 & T4Connection.INCONTEXT_OPT1_CLIENT_USERNAME) != 0)
		{
			buf.insertString(clientUserBytes);
		}

        insertXAttrsBuf(buf,inContext,ic);
		return buf;
    }
    // Add the length of the extended attribute here
    static int getXAttrsLength(InterfaceConnection ic) throws CharacterCodingException{


        int len = TRANSPORT.size_int * TRANSPORT.XATTRS_NUM +TRANSPORT.size_int; //types

        len +=  TRANSPORT.size_int;  //clipVarchar

        byte[] optionValueBytes = ic.encodeString("test", InterfaceUtilities.SQLCHARSETCODE_ISO88591);
        len +=  TRANSPORT.size_bytes(optionValueBytes);  //test

        len +=  TRANSPORT.size_int;  //sessionDebug
        return len;

    }
    //Add the value of the extended attribute here
    //The string "test" does nothing but an example.
    static void insertXAttrsBuf(LogicalByteArray buf, CONNECTION_CONTEXT_def inContext,InterfaceConnection ic)
        throws CharacterCodingException{


        buf.insertInt(TRANSPORT.XATTRS_NUM);
        buf.insertInt(TRANSPORT.XATTRS_SET_CLIPVARCHAR);
        buf.insertInt(inContext.clipVarchar);

        byte[] optionValueBytes = ic.encodeString("test", InterfaceUtilities.SQLCHARSETCODE_ISO88591);
        buf.insertInt(TRANSPORT.XATTRS_TEST);
        buf.insertString(optionValueBytes);	
	    buf.insertInt(TRANSPORT.XATTRS_SESSION_DEBUG);
        buf.insertInt(inContext.sessionDebug);

        }
}
