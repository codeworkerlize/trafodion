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

/**********************************************************
 * This class represents an address reference.
 *
 * @version 1.0
 **********************************************************/

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;

final class T4Address extends Address {

	private static final String t4ConnectionPrefix = "jdbc:t4jdbc:";
	private static final String urlPrefix = t4ConnectionPrefix + "//";
	private static final int minT4ConnectionAddrLen = t4ConnectionPrefix.length() + 4;
	private static final int AS_type = 1; // jdbc:subprotocol:subname

	private static String[] addressArr = null;
	private static Map<String,String> addressMap;

	/**
	 * The constructor.
	 * 
	 * @param addr
	 *            The addr has two forms:
	 * 
	 * DriverManager getConnection addr parameter format for connecting via the
	 * Fast JDBC Type 4 driver.
	 * 
	 * jdbc:subprotocol:subname
	 * 
	 * Where:
	 * 
	 * subprotocol = t4jdbc
	 * 
	 * subname = //<{IP Address|Machine Name}[:port]>/<properties>
	 * 
	 * Example: jdbc:t4jdbc://130.168.200.30:1433/database1
	 * 
	 */

	// ----------------------------------------------------------
	T4Address(T4Properties t4props, String addr) throws SQLException {
		super(t4props, addr);

		if (addr == null) {
			SQLException se = TrafT4Messages.createSQLException(m_t4props, "address_null_error");
			throw se;
		}

		//
		// We are now expecting addr = "//<{IP Address|Machine
		// Name}[:port]>/<properties>"
		//
		m_type = AS_type;

		//
		// We don't recognize this address syntax
		//
		if (acceptsURL(addr) == false) {
			SQLException se = TrafT4Messages.createSQLException(m_t4props, "address_parsing_error", addr);
			SQLException se2 = TrafT4Messages.createSQLException(m_t4props, "unknown_prefix_error");

			se.setNextException(se2);
			throw se;
		}

		if (isMultiAddr(addr)){
			m_properties = extractPropertiesFromString(addr);
			return;
		}
		//
		// We are now expecting addr = "<{IP Address|Machine Name}[:port]>"
		// Get the IP or Name
		//
		String IPorName = extractHostFromUrl(addr);
		if (isIPAddress(IPorName)) {
			m_ipAddress = IPorName;
		} else {
			m_machineName = IPorName;

			//
			// Get the port number if there is one.
			//
		}
		m_portNumber = new Integer(extractPortFromUrl(addr));
		m_properties = extractPropertiesFromString(addr);

		m_url = recreateAddress();

		validateAddress();
	}

	private boolean isMultiAddr(String url) throws SQLException {
        String regular = "([^/]*).*";
        return url.substring(T4Address.urlPrefix.length()).replaceFirst(regular, "$1").contains(",");
	}

	String recreateAddress() {
		String addr = null;

		addr = urlPrefix;

		if (m_machineName != null) {
			addr = addr + m_machineName;
		} else if (m_ipAddress != null) {
			addr = addr + m_ipAddress;

		}
		if (m_portNumber != null) {
			addr = addr + ":" + m_portNumber;

		}
		addr = addr + "/";

		return addr;
	} // end recreateAddress

	static boolean acceptsURL(String url) throws SQLException {
		try {
			return url.toLowerCase().startsWith(t4ConnectionPrefix);
		} catch (Exception ex) {
			throw new SQLException(ex.toString());
		}
	}

	static String extractUrlListFromUrl(String url, String addr) throws SQLException {

		int addressStartIndex = T4Address.urlPrefix.length();
		int addressEndIndex = url.indexOf('/', addressStartIndex);
		if (addressEndIndex == -1) {
			addressEndIndex = url.length();
		}
		String propVal = url.substring(addressEndIndex);
		return T4Address.urlPrefix + addr + propVal;
	}

	// jdbc:t4jdbc://192.168.0.11:23400,192.168.0.11/:key=value
	// jdbc:t4jdbc://[x:x:x:x]:23400,[x:x:x:x]/:key=value
	static List<String> extractUrlListFromUrl(String url) throws SQLException {
		List<String> urlList = new ArrayList<String>();

		int addressStartIndex = T4Address.urlPrefix.length();
		int addressEndIndex = url.indexOf('/', addressStartIndex);
		if (addressEndIndex == -1) {
			addressEndIndex = url.length();
		}

		String addresses = url.substring(addressStartIndex, addressEndIndex);
		String propVal = url.substring(addressEndIndex);
		addressMap = new HashMap<String,String>();
		String[] addressArr = addresses.split(",");
		setAddressArr(addressArr);

		for (int i = 0; i < addressArr.length; i++) {
			String address = addressArr[i];
			String port = "";
			//check if there has port
			if (address.lastIndexOf(":") > -1 && address.indexOf(":", address.indexOf("]")) > -1) {
				// get port in both ipv4 and ipv6
				port = address.substring(address.indexOf(":", address.indexOf("]")) + 1);
			}
			
			try {
				checkPort(port);
			} catch (SQLException e) {
				if (port.length() == 0) {
					address = address + ":" + T4Properties.getDefaultPort();
				}
			}

			urlList.add(T4Address.urlPrefix + address + propVal);
			addressMap.put(address,T4Address.urlPrefix + address + propVal);
		}
		
		return urlList;
	}

	// ----------------------------------------------------------
	String getUrl() {
          if (isIPv6ForPureUrl(getIPorName())){
              return urlPrefix + '[' + getIPorName() + ']' + ':' + getPort().toString() + "/:";
          }else
              return urlPrefix + getIPorName() + ':' + getPort().toString() + "/:";
	} // end getProps()

	// ----------------------------------------------------------
	Properties getProps() {
		return m_properties;
	} // end getProps()

	/**
	 * Return the host value
	 * 
	 * @param url
	 *            of format jdbc:t4jdbc://host:port/:[prop-name=prop-value]..
	 * @return host string
	 */
	private String extractHostFromUrl(String url) throws SQLException {
		if (url == null) {
			SQLException se = TrafT4Messages.createSQLException(m_t4props, "address_parsing_error", url);
			SQLException se2 = TrafT4Messages.createSQLException(m_t4props, "address_format_2_error");
			se.setNextException(se2);
			throw se;

		}
		if (url.length() < minT4ConnectionAddrLen) {
			SQLException se = TrafT4Messages.createSQLException(m_t4props, "address_parsing_error", url);
			SQLException se2 = TrafT4Messages.createSQLException(m_t4props, "min_address_length_error");

			se.setNextException(se2);
			throw se;
		}

		int hostStartIndex = urlPrefix.length();
		int hostEndIndex = -1;
		if (isIPV6(url)) {
			hostStartIndex = hostStartIndex + 1;
			hostEndIndex = url.lastIndexOf(']'); // IP6
		} else {
			hostEndIndex = url.indexOf(':', hostStartIndex); // IP4

		}
		if (hostEndIndex < 0) {
			SQLException se = TrafT4Messages.createSQLException(m_t4props, "address_parsing_error", url);
			SQLException se2 = TrafT4Messages.createSQLException(m_t4props, "address_format_error", url);

			se.setNextException(se2);
			throw se;
		}

		String host = url.substring(hostStartIndex, hostEndIndex);
		if ((host == null) || (host.length() == 0)) {
			SQLException se = TrafT4Messages.createSQLException(m_t4props, "address_parsing_error", url);
			SQLException se2 = TrafT4Messages.createSQLException(m_t4props, "address_format_error");
			SQLException se3 = TrafT4Messages.createSQLException(m_t4props, "missing_ip_or_name_error");
			se.setNextException(se2);
			se2.setNextException(se3);
			throw se;
		}

		return host;
	}

	/**
	 * Return the port value
	 * 
	 * @param url
	 *            of format jdbc:t4jdbc://host:port/:[prop-name=prop-value]..
	 * @return port string
	 */
	private String extractPortFromUrl(String url) throws SQLException {
		int portStartIndex = 0;
		int portEndIndex = 0;
		if (isIPV6(url)){
			portStartIndex = url.indexOf(':', url.indexOf(']')) + 1;
			portEndIndex = url.indexOf('/', portStartIndex);
			if (portEndIndex < 0) {
				portEndIndex = url.length();

			}
		}else{
			portStartIndex = url.indexOf(':', urlPrefix.length()) + 1;
			portEndIndex = url.indexOf('/', portStartIndex);
			if (portEndIndex < 0) {
				portEndIndex = url.length();

			}
		}

		String port = url.substring(portStartIndex, portEndIndex);
        try {
            checkPort(port);
        } catch (Exception e) {
            port = T4Properties.getDefaultPort() + "";
        }

		return port;
	}
	
	/**
	 * Checks if the port is correct
	 */
	private static void checkPort(String port) throws SQLException {
		if (port.length() < 1) {
			SQLException se = TrafT4Messages.createSQLException(null, "port_length_error", port);
			throw se;
		}

		int asPort;
		try {
			asPort = Integer.parseInt(port);
		} catch (NumberFormatException e) {
			SQLException se = TrafT4Messages.createSQLException(null, "port_type_error", port);
			throw se;
		}

		if ((asPort < 0) || (asPort > 65535)) {
			SQLException se = TrafT4Messages.createSQLException(null, "port_range_error", port);
			throw se;
		}
	}

	/**
	 * Checks if the url is of IP6 protocol
	 */
	private boolean isIPV6(String url) throws SQLException {
		int hostStartIndex = urlPrefix.length();
		return (url.charAt(hostStartIndex) == '[');
	}

	/**
	 * Extracts the property name, value pair from a url String, seperated by ;
	 * 
	 * @param url
	 *            of format jdbc:t4jdbc://host:port/:[prop-name=prop-value]..
	 * @return Propeties object
	 * @throws IOException
	 */
	private Properties extractPropertiesFromString(String url) throws SQLException {
		int urLength = url.length();
		int hostStartIndex = urlPrefix.length();
		int propStartIndex = url.indexOf('/', hostStartIndex);
		if (propStartIndex < 0) {
			return null;
		}

		if (propStartIndex == urLength) {
			return null;
		}

		if (url.charAt(propStartIndex) == '/') {
			propStartIndex++;

		}
		if (propStartIndex == urLength) {
			return null;
		}

		if (url.charAt(propStartIndex) == ':') {
			propStartIndex++;

		}
		if (propStartIndex == urLength) {
			return null;
		}

		String propStr = url.substring(propStartIndex);
		if ((propStr == null) || (propStr.length() == 0)) {
			return null;
		}

		Properties props = new Properties();
		propStr = propStr.replace(';', '\n');
		ByteArrayInputStream byteArrIPStream = new ByteArrayInputStream(propStr.getBytes());

		try {
			props.load(byteArrIPStream);
		} catch (IOException ioex) {
			throw new SQLException(ioex.getMessage());
		}

		return props;
	}

	/**
	 * Checks the string is host or port.
	 * 
	 * @param IPorName
	 * @return true if the address is a IP address
	 */
	private boolean isIPAddress(String IPorName) {

		return isIPv4(IPorName) || isIPv6ForPureUrl(IPorName);
	}

	public boolean isIPv4(String str) {
		if (!Pattern.matches("[0-9]+[.][0-9]+[.][0-9]+[.][0-9]+", str))
			return false;
		else {
			String[] arrays = str.split("\\.");
			if (Integer.parseInt(arrays[0]) < 256 && arrays[0].length() <= 3
					&& Integer.parseInt(arrays[1]) < 256 && arrays[1].length() <= 3
					&& Integer.parseInt(arrays[2]) < 256 && arrays[2].length() <= 3
					&& Integer.parseInt(arrays[3]) < 256 && arrays[3].length() <= 3)
				return true;
			else return false;
		}
	}

	public boolean isIPv6ForPureUrl(String str) {

		return isIPV6Std(str) || isIPV6Compress(str);
	}

	public boolean isIPV6Std(String str) {
		if (!Pattern.matches("^(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$", str))
			return false;
		return true;
	}

	public boolean isIPV6Compress(String str) {
		if (!Pattern.matches(                "^((?:[0-9A-Fa-f]{1,4}(?::[0-9A-Fa-f]{1,4})*)?)::((?:[0-9A-Fa-f]{1,4}(?::[0-9A-Fa-f]{1,4})*)?)$", str))
			return false;
		return true;
	}

	static String[] getAddressArr() {
		return addressArr;
	}

	static void setAddressArr(String[] addressArr) {
		T4Address.addressArr = addressArr;
	}

	static Map<String, String> getAddressMap() {
		return addressMap;
	}
} // end class Address
