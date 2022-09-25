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
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

public class T4LoggingUtilities {

	static final Long m_syncL = new Long(1L);

	// ----------------------------------------------------------
	public T4LoggingUtilities() {
	}

	// ----------------------------------------------------------
	static String getUniqueID() {
		synchronized (m_syncL) {
			try {
				Thread.sleep(m_syncL.longValue()); // wait 1 millisecond
			} catch (Exception e) {
			}
		}

		java.util.Date d1 = new java.util.Date();
		long t1 = d1.getTime();
		String name = null;

		name = Long.toString(t1);
		return name;
	}

	// ----------------------------------------------------------
	static String getUniqueLogFileName(String uniqueID) {
		String name = null;

		name = "%h/t4jdbc" + uniqueID + ".log";
		return name;
	}

	// ----------------------------------------------------------
	static String getUniqueLoggerName(String uniqueID) {
		String name = null;

		name = "org.trafodion.jdbc.t4.logger" + uniqueID;
		return name;
	}

	// ----------------------------------------------------------
	static Object[] makeParams() {
		return null;
	} // end makeParams

    // ----------------------------------------------------------
   static Object[] makeParams(T4Properties t4props, Object... obj) {

        Object newObj[] = new Object[obj == null ? 1 : obj.length + 1];

        newObj[0] = t4props;
        if (obj != null) {
            for (int i = 0; i < obj.length; i++) {
                newObj[i + 1] = obj[i];
            }
        }

        return newObj;
    } // end makeParams

    // this method is the first step to reconsitution the log mechainism in t4
    public static void log(T4Properties t4props, Level l, String msg, Object... p) {
        try {
            if ((! t4props.t4Logger_.isLoggable(l)) && (t4props.getLogWriter() == null))
               return; 
        } catch (SQLException e1) {
            return;
        }
        String clazz = Thread.currentThread().getStackTrace()[1].getClassName();
        String method = Thread.currentThread().getStackTrace()[1].getMethodName();
        StackTraceElement[] steArray = new Throwable().getStackTrace();

        int selfIndex = -1;
        for (int i = 0; i < steArray.length; i++) {
            final String className = steArray[i].getClassName();
            if (className.equals(clazz)) {
                selfIndex = i;
                break;
            }
        }

        int found = -1;
        for (int i = selfIndex + 1; i < steArray.length; i++) {
            final String methodName = steArray[i].getMethodName();
            if (!(methodName.equals(method))) {
                found = i;
                break;
            }
        }

        if (found != -1) {
            StackTraceElement ste = steArray[found];
            // setting the class name has the side effect of setting
            // the needToInferCaller variable to false.
            clazz = ste.getFileName()+":"+ste.getLineNumber();
            method = ste.getMethodName();

        }

        for (int i = 0; i < steArray.length; i++) {
            steArray[i] = null;
        }
        if (t4props.t4Logger_.isLoggable(l)) {
            t4props.t4Logger_.logp(l, clazz, method, msg,
                    T4LoggingUtilities.makeParams(t4props, p));

            try {
                if (t4props.getLogWriter() != null) {
                    LogRecord lr = new LogRecord(l, msg);
                    Object param[] = T4LoggingUtilities.makeParams(t4props, p);
                    lr.setParameters(param);
                    lr.setSourceClassName(clazz);
                    lr.setSourceMethodName(method);
                    T4LogFormatter lf = new T4LogFormatter();
                    String temp = lf.format(lr);
                    t4props.getLogWriter().println(temp);
                }
            } catch (SQLException e) {
            }
        }
        clazz = null;
        method = null;
        msg = null;
    }
    static void log(Logger logger, Level l, T4Properties t4props, String clazz, String method, String msg, Object... p) {
        if (logger.isLoggable(l) == true) {
            logger.logp(l, clazz, method, msg, T4LoggingUtilities.makeParams(t4props, p));
        }
    }
} // end class T4LogFormatter
