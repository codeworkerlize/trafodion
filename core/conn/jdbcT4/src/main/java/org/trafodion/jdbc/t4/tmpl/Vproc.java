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

package org.trafodion.jdbc.t4.tmpl;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;

public class Vproc {
	private static Vproc vproc = new Vproc();
	private StringBuilder vprocStr = new StringBuilder();
	public static final int jdbcMajorVersion = 13;
	public static final int jdbcMinorVersion = 0;

	public static void main(String[] args) throws IOException {
		System.out.println(Vproc.getVproc());
	}

	public static String getVproc() {
		return vproc.vprocStr.toString();
	}

	private Vproc() {
		vprocStr.setLength(0);
		vprocStr.append("Traf_JDBC_Type");

		InputStream in = Vproc.class.getResourceAsStream("/jdbct4.esg.properties");
		Properties config = new Properties();
		try {
			config.load(in);

			for (Map.Entry entry : config.entrySet()) {
				vprocStr.append('_');
				vprocStr.append(entry.getValue());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
