// @@@ START COPYRIGHT @@@
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.
//
// @@@ END COPYRIGHT @@@

package org.trafodion.jdbc.t4;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.LogRecord;
import org.trafodion.jdbc.t4.utils.Utils;

public class T4LogFormatter extends java.util.logging.Formatter {

    private static final int DEFAULT_LOG_LINE_LEN_BYTES = 500;
    private static final String L_ROUND_BRACKET = "(";
    private static final String R_ROUND_BRACKET = ")";
    private static final String L_SQUARE_BRACKET = "[";
    private static final String R_SQUARE_BRACKET = "]";
    private static final String SEPARATOR = " ~ ";
    private static final String QUOTATION_MARK = "\"";
    private static final String COMMA = ", ";
    private static final String BLANK = " ";
    private static final String DOT = ".";
//    private static final DecimalFormat DF = new DecimalFormat("########################################################00000000");

    // ----------------------------------------------------------
    public T4LogFormatter() {}

    // ----------------------------------------------------------
    public String format(LogRecord lr) {
        StringBuilder m1 = new StringBuilder(DEFAULT_LOG_LINE_LEN_BYTES);
        Object params[] = lr.getParameters();

        try {
            //
            // By convension, the first parameter is a TrafT4Connection object or
            // a T4Properties object
            //
            T4Properties tp = null;

            if (params != null && params.length > 0) {
                if (params[0] instanceof TrafT4Connection)
                    tp = ((TrafT4Connection) params[0]).getT4props();
                else
                    tp = (T4Properties) params[0];
            }
            // String connection_id = "";
            // String server_id = "";
            String dialogue_id = "";
            if (tp != null) {
                // connection_id = tp.getConnectionID();
                // server_id = tp.getServerID();
                dialogue_id = tp.getDialogueID() == null ? "" : tp.getDialogueID();
            }

            //
            // Format for message:
            //
            // time-stamp, T[thread-id], D[dialogue-id], file:line, method(parameters) ~ text
            // 2019-07-17 18:57:15.730, FINER, T[1], Dial[193879907], TrafT4Connection.java:31, getConnection() ~ Connection process successful
            Date d1 = new Date(lr.getMillis());
            DateFormat df1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.S");
            String time_stamp = df1.format(d1);

            m1.append(time_stamp).append(COMMA).append(lr.getLevel()).append(COMMA).append("T")
                    .append(L_SQUARE_BRACKET).append(lr.getThreadID()).append(R_SQUARE_BRACKET)
                    .append(COMMA).append("D").append(L_SQUARE_BRACKET).append(dialogue_id)
                    .append(R_SQUARE_BRACKET).append(COMMA).append(lr.getSourceClassName())
                    .append(DOT).append(lr.getSourceMethodName()).append(L_ROUND_BRACKET);


            if (params != null) {
                Object tempParam = null;
                String paramText = null;

                //
                // Skip the first parameter, which is a T4Connection, and is
                // handled above.
                //
                for (int i = 1; i < params.length; i++) {
                    tempParam = params[i];
                    if (tempParam != null) {
                        //
                        // If the parameter is an array, try to print each
                        // element of the array.
                        //
                        if (tempParam.getClass().isArray()) {
                            paramText = makeObjectArray(tempParam);
                        } else {
                            paramText = tempParam.toString();
                        }
                    } else {
                        paramText = "null";

                    }
                    m1.append(QUOTATION_MARK).append(paramText).append(QUOTATION_MARK);
                    if (i + 1 < params.length) {
                        m1.append(COMMA);
                    }
                }
            }
            m1.append(R_ROUND_BRACKET).append(SEPARATOR).append(lr.getMessage())
                    .append(Utils.lineSeparator());

        } catch (Exception e) {
            //
            // Tracing should never cause an internal error, but if it does, we
            // do want to
            // capture it here. An internal error here has no effect on the user
            // program,
            // so we don't want to throw an exception. We'll put the error in
            // the trace log
            // instead, and instruct the user to report it
            //
            m1.append(
                    "An internal error has occurred in the tracing logic. Please report this to your representative. \n")
                    .append(" exception = ").append(e.toString()).append("\n message = ")
                    .append(e.getMessage()).append("\n  Stack trace = \n");

            StackTraceElement st[] = e.getStackTrace();

            for (int i = 0; i < st.length; i++) {
                m1.append("    ").append(st[i].toString()).append("\n");
            }
            m1.append("\n");
        } // end catch

        //
        // The params array is reused, so we must null it out before returning.
        //
        if (params != null) {
            for (int i = 0; i < params.length; i++) {
                params[i] = null;
            }
        }

        return m1.toString();

    } // end formatMessage

    // ---------------------------------------------------------------------
    String makeObjectArray(Object obj) {
        StringBuilder sb = new StringBuilder(1024);

        if (obj instanceof boolean[]) {
            boolean[] temp = (boolean[]) obj;
            for (int i = 0; i < temp.length; i++)
                sb.append(L_SQUARE_BRACKET).append(i).append(R_SQUARE_BRACKET).append(temp[i])
                        .append(BLANK);
        } else if (obj instanceof char[]) {
            char[] temp = (char[]) obj;
            for (int i = 0; i < temp.length; i++)
                sb.append(L_SQUARE_BRACKET).append(i).append(R_SQUARE_BRACKET).append(temp[i])
                        .append(BLANK);
        } else if (obj instanceof byte[]) {
            byte[] temp = (byte[]) obj;
            for (int i = 0; i < temp.length; i++)
                sb.append(L_SQUARE_BRACKET).append(i).append(R_SQUARE_BRACKET).append(temp[i])
                        .append(BLANK);
        } else if (obj instanceof short[]) {
            short[] temp = (short[]) obj;
            for (int i = 0; i < temp.length; i++)
                sb.append(L_SQUARE_BRACKET).append(i).append(R_SQUARE_BRACKET).append(temp[i])
                        .append(BLANK);
        } else if (obj instanceof int[]) {
            int[] temp = (int[]) obj;
            for (int i = 0; i < temp.length; i++)
                sb.append(L_SQUARE_BRACKET).append(i).append(R_SQUARE_BRACKET).append(temp[i])
                        .append(BLANK);
        } else if (obj instanceof long[]) {
            long[] temp = (long[]) obj;
            for (int i = 0; i < temp.length; i++)
                sb.append(L_SQUARE_BRACKET).append(i).append(R_SQUARE_BRACKET).append(temp[i])
                        .append(BLANK);
        } else if (obj instanceof float[]) {
            float[] temp = (float[]) obj;
            for (int i = 0; i < temp.length; i++)
                sb.append(L_SQUARE_BRACKET).append(i).append(R_SQUARE_BRACKET).append(temp[i])
                        .append(BLANK);
        } else if (obj instanceof double[]) {
            double[] temp = (double[]) obj;
            for (int i = 0; i < temp.length; i++)
                sb.append(L_SQUARE_BRACKET).append(i).append(R_SQUARE_BRACKET).append(temp[i])
                        .append(BLANK);
        } else {
            Object[] temp = (Object[]) obj;
            for (int i = 0; i < temp.length; i++) {
                sb.append(L_SQUARE_BRACKET).append(i).append(R_SQUARE_BRACKET).append(temp[i])
                        .append(BLANK);
            }
        }
        return sb.toString();
    } // end makeObjectArray

} // end class T4LogFormatter
