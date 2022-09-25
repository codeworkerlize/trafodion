/**
* @@@ START COPYRIGHT @@@

Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.

* @@@ END COPYRIGHT @@@
 */
package org.trafodion.dcs.master.listener;

public final class ListenerConstants {
    // read buffer stat
    public static final int BUFFER_INIT = 0;
    public static final int HEADER_PROCESSED = 1;
    public static final int BODY_PROCESSED = 2;
    // errors returned by DcsMaster
    public static final int DcsMasterParamError_exn = 1;
    public static final int DcsMasterTimeout_exn = 2;
    public static final int DcsMasterNoSrvrHdl_exn = 3;
    public static final int DcsMasterTryAgain_exn = 4;
    public static final int DcsMasterASNotAvailable_exn = 5;
    public static final int DcsMasterDSNotAvailable_exn = 6;
    public static final int DcsMasterPortNotAvailable_exn = 7;
    public static final int DcsMasterInvalidUser_exn = 8;
    public static final int DcsMasterLogonUserFailure_exn = 9;
    public static final int DcsMasterInvalidTenant_exn = 10;
    public static final int DcsMasterSessionLimit_exn = 11;
    public static final int DcsMasterTenantSessionLimit_exn = 12;
    public static final int DcsMasterIpwhitelist_exn = 13;
    public static final int DcsMasterDistributionSevr_exn = 14;
    //
    public static final int REQUST_CLOSE = 1;
    public static final int REQUST_WRITE = 2;
    public static final int REQUST_WRITE_EXCEPTION = 3;
    //
    // Fixed values taken from TransportBase.h
    //
    public static final int CLIENT_HEADER_VERSION_BE = 101;
    public static final int CLIENT_HEADER_VERSION_LE = 102;
    public static final int SERVER_HEADER_VERSION_BE = 201;
    public static final int SERVER_HEADER_VERSION_LE = 202;

    public static final byte YES = 'Y';
    public static final byte NO = 'N';

    // header size
    public static final int HEADER_SIZE = 40;
    // max body size
    public static final int BODY_SIZE = 1024 * 5;
    //
    public static final int LITTLE_ENDIAN_SIGNATURE = 959447040; // 0x39300000
    public static final int BIG_ENDIAN_SIGNATURE = 12345; // 0x00003039

    public static final int DCS_MASTER_COMPONENT = 2;
    public static final int ODBC_SRVR_COMPONENT = 4;

    public static final int DCS_MASTER_VERSION_MAJOR_1 = 3;
    public static final int DCS_MASTER_VERSION_MINOR_0 = 0;
    public static final int DCS_MASTER_BUILD_1 = 1;

    public static final int CHARSET = 268435456; // (2^28) For charset changes compatibility
    public static final int PASSWORD_SECURITY = 67108864; // (2^26)
    public static final int PASSWORD_SECURITY_BASE64 = 0x40000000;

    // =================MXOSRVR versions ===================
    public static final int MXOSRVR_ENDIAN = 256;
    public static final int MXOSRVR_VERSION_MAJOR = 3;
    public static final int MXOSRVR_VERSION_MINOR = 5;
    public static final int MXOSRVR_VERSION_BUILD = 1;

    public static final short DCS_MASTER_GETSRVRAVAILABLE = 1000 + 19;
    public static final short DCS_MASTER_CANCELQUERY = 1000 + 13;
    public static final short DCS_MASTER_KEEPALIVECHECK = 1000 + 20;
    public static final short DCS_MASTER_CHECKACTIVEMASTER = 1000 + 21;

    public static final int JDBC_DRVR_COMPONENT = 20;
    public static final int ODBC_DRVR_COMPONENT = 26;
    public static final int LINUX_ODBC_DRVR_COMPONENT = 27;

    public static final short DCS_VERSION = 7;

}
