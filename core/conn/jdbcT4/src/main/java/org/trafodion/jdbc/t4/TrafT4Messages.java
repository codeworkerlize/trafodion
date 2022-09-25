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

import java.sql.SQLWarning;
import java.text.MessageFormat;
import java.util.Locale;
import java.util.MissingResourceException;
import java.util.PropertyResourceBundle;
import java.util.ResourceBundle;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.trafodion.jdbc.t4.utils.Utils;

class TrafT4Messages {
    static Logger getMessageLogger(T4Properties t4props) {
        return (t4props != null) ? t4props.t4Logger_ : T4Properties.t4GlobalLogger;
    }

    static int getErrorCode(T4Properties t4props, String messageId) {
        Locale currentLocale = t4props == null ? null : t4props.getLocale();

        PropertyResourceBundle messageBundle =
                (PropertyResourceBundle) ResourceBundle.getBundle("T4Messages", currentLocale);
        String sqlcodeStr = messageBundle.getString(messageId + "_sqlcode");
        int sqlcode = 1;
        if (sqlcodeStr != null) {
            try {
                sqlcode = Integer.parseInt(sqlcodeStr);
            } catch (NumberFormatException e1) {
                // use 1 as default
            }
        }

        return -sqlcode;
    }

    static SQLWarning createSQLWarning(T4Properties t4props, String messageId, Object[] messageArguments) {
        Logger log = getMessageLogger(t4props);

        Locale currentLocale = t4props == null ? null : t4props.getLocale();
        currentLocale = currentLocale == null ? Locale.getDefault() : currentLocale;

        int sqlcode = 1;
        SQLWarning ret = null;

        try {
            PropertyResourceBundle messageBundle = (PropertyResourceBundle) ResourceBundle.getBundle("T4Messages",
                    currentLocale);

            MessageFormat formatter = new MessageFormat("");
            formatter.setLocale(currentLocale);
            formatter.applyPattern(messageBundle.getString(messageId + "_msg"));

            String message = formatter.format(messageArguments);
            String sqlState = messageBundle.getString(messageId + "_sqlstate");
            String sqlcodeStr = messageBundle.getString(messageId + "_sqlcode");

            if (sqlcodeStr != null) {
                try {
                    sqlcode = Integer.parseInt(sqlcodeStr);
                } catch (NumberFormatException e1) {
                    // use 1 as default
                }
            }
            if (log != null && log.isLoggable(Level.WARNING)) {
                log.logp(Level.WARNING, "TrafT4Messages", "createSQLWarning", message, t4props);
            }
            ret = new SQLWarning(message, sqlState, sqlcode);
        } catch (MissingResourceException e) {
            // If the resource bundle is not found, concatenate the messageId
            // and the parameters
            String message;
            int i = 0;

            message = "The message id: " + messageId;
            if (messageArguments != null && messageArguments.length > 0) {
                message = message.concat(" With parameters: ");
                while (true) {
                    message = message.concat(messageArguments[i++].toString());
                    if (i >= messageArguments.length) {
                        break;
                    } else {
                        message = message.concat(",");
                    }
                }
            } // end if
            if (log != null && log.isLoggable(Level.FINER)) {
                log.logp(Level.FINER, "TrafT4Messages", "createSQLWarning", message);
            }
            ret = new SQLWarning(message, "01000", 1);
        }

        return ret;
    }

    static void setSQLWarning(T4Properties t4props, TrafT4Handle handle, SQLWarningOrError[] we1) {
        Logger log = getMessageLogger(t4props);

        int curErrorNo;
        SQLWarning sqlWarningLeaf;

        if (we1.length == 0) {
            handle.setSqlWarning(null);
            return;
        }

        for (curErrorNo = 0; curErrorNo < we1.length; curErrorNo++) {
            if (log != null && log.isLoggable(Level.WARNING)) {
                String tmp = getRemoteProcess(t4props) + "Text: " + we1[curErrorNo].text
                        + ". SQLState: " + we1[curErrorNo].sqlState + ". SQLCode: "
                        + we1[curErrorNo].sqlCode;
                log.logp(Level.WARNING, "TrafT4Messages", "setSQLWarning", tmp);
            }
            if (!we1[curErrorNo].isWarning){
                continue;
            }
            sqlWarningLeaf = new SQLWarning(we1[curErrorNo].text, we1[curErrorNo].sqlState, we1[curErrorNo].sqlCode);
            handle.setSqlWarning(sqlWarningLeaf);
        } // end for
        return;
    }
    static void setSQLWarning(T4Properties t4props, TrafT4Handle handle,String text) {
        Logger log = getMessageLogger(t4props);

        SQLWarning sqlWarningLeaf;

        sqlWarningLeaf = new SQLWarning(text);
        handle.setSqlWarning(sqlWarningLeaf);

        return;
    }


    static void setSQLWarning(T4Properties t4props, TrafT4Handle handle, SQLWarningOrError we1) {
        Logger log = getMessageLogger(t4props);

        SQLWarning sqlWarningLeaf;

        if (log != null && log.isLoggable(Level.WARNING)) {
            String tmp = getRemoteProcess(t4props) + "Text: " + we1.text + ". SQLState: "
                    + we1.sqlState + ". SQLCode: " + we1.sqlCode;
            log.logp(Level.WARNING, "TrafT4Messages", "setSQLWarning", tmp);
        }

        sqlWarningLeaf = new SQLWarning(we1.text, we1.sqlState, we1.sqlCode);
        handle.setSqlWarning(sqlWarningLeaf);

        return;
    }

    static void setSQLWarning(T4Properties t4props, TrafT4Handle handle, ERROR_DESC_LIST_def sqlWarning) {
        Logger log = getMessageLogger(t4props);

        int curErrorNo;
        ERROR_DESC_def error_desc_def[];
        SQLWarning sqlWarningLeaf;

        if (sqlWarning.length == 0) {
            handle.setSqlWarning(null);
            return;
        }

        error_desc_def = sqlWarning.buffer;
        for (curErrorNo = 0; curErrorNo < sqlWarning.length; curErrorNo++) {
            if (log != null && log.isLoggable(Level.WARNING)) {
                String tmp =
                        getRemoteProcess(t4props) + "Text: " + error_desc_def[curErrorNo].errorText
                                + ". SQLState: " + error_desc_def[curErrorNo].sqlstate
                                + ". SQLCode: " + error_desc_def[curErrorNo].sqlcode;
                log.logp(Level.WARNING, "TrafT4Messages", "setSQLWarning", tmp);
            }

            sqlWarningLeaf = new SQLWarning(getRemoteProcess(t4props) + error_desc_def[curErrorNo].errorText, error_desc_def[curErrorNo].sqlstate,
                    error_desc_def[curErrorNo].sqlcode);
            handle.setSqlWarning(sqlWarningLeaf);
        }
        return;
    } // end setSQLWarning

    // ------------------------------------------------------------------------------------------------
    static void throwSQLException(T4Properties t4props, ERROR_DESC_LIST_def SQLError) throws TrafT4Exception {
        Logger log = getMessageLogger(t4props);

        TrafT4Exception sqlException = null;
        TrafT4Exception sqlExceptionHead = null;
        int curErrorNo;

        if (SQLError.length == 0) {
            throw createSQLException(t4props, getRemoteProcess(t4props) + "No messages in the Error description");
        }

        for (curErrorNo = 0; curErrorNo < SQLError.length; curErrorNo++) {
            if (log != null) {
                String tmp = getRemoteProcess(t4props) + "Text: "
                        + SQLError.buffer[curErrorNo].errorText + ". SQLState: "
                        + SQLError.buffer[curErrorNo].sqlstate + ". SQLCode: "
                        + SQLError.buffer[curErrorNo].sqlcode;

                log.logp(Level.SEVERE, "TrafT4Messages", "throwSQLException", tmp);
            }

            if (SQLError.buffer[curErrorNo].errorCodeType == TRANSPORT.ESTIMATEDCOSTRGERRWARN) {
                //
                // NCS said it was an SQL error, but it really wasn't it was a
                // NCS resource governing error
                //
                sqlException = createSQLException(t4props, "resource_governing");
            } else {
                sqlException = new TrafT4Exception(
                        getRemoteProcess(t4props) + SQLError.buffer[curErrorNo].errorText,
                        SQLError.buffer[curErrorNo].sqlstate, SQLError.buffer[curErrorNo].sqlcode,
                        null);
            }
            if (curErrorNo == 0) {
                sqlExceptionHead = sqlException;
            } else {
                sqlExceptionHead.setNextException(sqlException);
            }
        }

        throw sqlExceptionHead;
    }

    // ------------------------------------------------------------------------------------------------

    static void throwSQLException(T4Properties t4props, TrafT4Handle handle, SQLWarningOrError[] we1) throws TrafT4Exception {
        Logger log = getMessageLogger(t4props);

        TrafT4Exception sqlException = null;
        TrafT4Exception sqlExceptionHead = null;
        int curErrorNo;

        if (we1.length == 0) {
            throw createSQLException(t4props, getRemoteProcess(t4props) + "No messages in the Error description");
        }

        boolean hasHeadException = false;
        for (curErrorNo = 0; curErrorNo < we1.length; curErrorNo++) {
            if (log != null) {
                String tmp = getRemoteProcess(t4props) + "Text: " + we1[curErrorNo].text
                        + ". SQLState: " + we1[curErrorNo].sqlState + ". SQLCode: "
                        + we1[curErrorNo].sqlCode;

                log.logp(Level.SEVERE, "TrafT4Messages", "throwSQLException", tmp);
            }
            if(we1[curErrorNo].isWarning){
                setSQLWarning(t4props, handle, we1);
                continue;
            }
            sqlException = new TrafT4Exception(getRemoteProcess(t4props) + we1[curErrorNo].text,
                    we1[curErrorNo].sqlState, we1[curErrorNo].sqlCode, null);

            if (!hasHeadException){
                sqlExceptionHead = sqlException;
                hasHeadException = true;
            } else {
                sqlExceptionHead.setNextException(sqlException);
            }
        } // end for

        throw sqlExceptionHead;
    } // end throwSQLException

    static void throwSQLException(T4Properties t4props, SQLWarningOrError[] we1) throws TrafT4Exception {
        Logger log = getMessageLogger(t4props);
        TrafT4Exception sqlException = null;
        TrafT4Exception sqlExceptionHead = null;
        int curErrorNo;

        if (we1.length == 0) {
            throw createSQLException(t4props, getRemoteProcess(t4props) + "No messages in the Error description");
        }

        boolean hasHeadException = false;
        for (curErrorNo = 0; curErrorNo < we1.length; curErrorNo++) {
            if(we1[curErrorNo].isWarning){
                continue;
            }
            if (log != null) {
                String tmp = getRemoteProcess(t4props) + "Text: " + we1[curErrorNo].text + ". SQLState: "
                        + we1[curErrorNo].sqlState + ". SQLCode: " + we1[curErrorNo].sqlCode;
                log.logp(Level.SEVERE, "TrafT4Messages", "throwSQLException", tmp);
            }
            sqlException = new TrafT4Exception(getRemoteProcess(t4props) + we1[curErrorNo].text,
                    we1[curErrorNo].sqlState, we1[curErrorNo].sqlCode, null);

            if (!hasHeadException){
                sqlExceptionHead = sqlException;
                hasHeadException = true;
            } else {
                sqlExceptionHead.setNextException(sqlException);
            }
        } // end for

        throw sqlExceptionHead;
    } // end throwSQLException

    private static String getRemoteProcess(T4Properties t4props) {
        String remoteProcess =
                t4props == null || t4props.getRemoteProcess() == null ? "Not connect yet. "
                        : "Remote Process: " + t4props.getRemoteProcess() + ". ";
        return remoteProcess;
    }

    static TrafT4Exception createSQLException(T4Properties t4props, String messageId,
            Object ...messageArguments) {
        if (t4props == null) {
            return createSQLException(null, null, messageId, messageArguments);
        } else {
            return createSQLException(t4props, t4props.getLocale(), messageId, messageArguments);
        }
    }

    // ------------------------------------------------------------------------------------------------
    /**
     * this method is deprecated, the param <b>Locale</b> has no needed , as it can be get in
     * <b>t4props</b>, if there need to set <b>Locale</b>, programmers can use
     * <p>
     * t4props.setLanguage(language);
     * </p>
     * then invoke {@link #createSQLException(t4props , messageId, messageArguments...) }
     *
     * @see java.util.Locale
     * @param t4props
     * @param msgLocale
     * @param messageId
     * @param messageArguments
     * @return
     */
    @Deprecated
    private static TrafT4Exception createSQLException(T4Properties t4props, Locale msgLocale, String messageId,
            Object[] messageArguments) {
        Logger log = getMessageLogger(t4props);

        Locale currentLocale = t4props == null ? null : t4props.getLocale();
        currentLocale = currentLocale == null ? Locale.getDefault(): currentLocale;

        int sqlcode;
        try {
            PropertyResourceBundle messageBundle = (PropertyResourceBundle) ResourceBundle.getBundle("T4Messages",
                    currentLocale);

            MessageFormat formatter = new MessageFormat("");
            formatter.setLocale(currentLocale);
            formatter.applyPattern(messageBundle.getString(messageId + "_msg"));

            String message = formatter.format(messageArguments);
            String sqlState = messageBundle.getString(messageId + "_sqlstate");
            String sqlcodeStr = messageBundle.getString(messageId + "_sqlcode");

            if (sqlcodeStr != null) {
                try {
                    sqlcode = Integer.parseInt(sqlcodeStr);
                    sqlcode = -sqlcode;
                } catch (NumberFormatException e1) {
                    sqlcode = -1;
                }
            } else {
                sqlcode = -1;

            }

            if (log.isLoggable(Level.SEVERE)) {
                StackTraceElement[] stackTrace= new Throwable().getStackTrace();
                StringBuilder sb = new StringBuilder();
                sb.append(getRemoteProcess(t4props)).append(message);
                for (StackTraceElement traceElement : stackTrace) {
                    sb.append(Utils.lineSeparator()).append("at " + traceElement);
                }
                log.logp(Level.SEVERE, "TrafT4Messages", "createSQLException", sb.toString(), t4props);
            }
            return new TrafT4Exception(getRemoteProcess(t4props) + message, sqlState, sqlcode, messageId);
        } catch (MissingResourceException e) {
            // If the resource bundle is not found, concatenate the messageId
            // and the parameters
            String message;
            int i = 0;
            message = getRemoteProcess(t4props) + "The message id: " + messageId;
            if (messageArguments != null && messageArguments.length > 0) {
                message = message.concat(" With parameters: ");
                while (true) {
                    message = message.concat(messageArguments[i++] + "");
                    if (i >= messageArguments.length) {
                        break;
                    } else {
                        message = message.concat(",");
                    }
                }
            } // end if
            if (log.isLoggable(Level.SEVERE)) {
                StackTraceElement[] stackTrace= new Throwable(message).getStackTrace();
                StringBuilder sb = new StringBuilder();

                for (StackTraceElement traceElement : stackTrace) {
                    sb.append("\tat " + traceElement);
                }
                log.logp(Level.SEVERE, "TrafT4Messages", "createSQLException", sb.toString());
            }
            return new TrafT4Exception(message, "HY000", -1, messageId);
        } // end catch
    } // end createSQLException

    // ------------------------------------------------------------------------------------------------
    static void throwUnsupportedFeatureException(T4Properties t4props, Object... messageArguments) throws TrafT4Exception {
        throw TrafT4Messages.createSQLException(t4props, "unsupported_feature", messageArguments);
    } // end throwUnsupportedFeatureException

    static void throwUnsupportedMethodException(T4Properties t4props, Object... messageArguments) throws TrafT4Exception {
        throw TrafT4Messages.createSQLException(t4props, "unsupported_method", messageArguments);
    }

    // ------------------------------------------------------------------------------------------------
    static void throwDeprecatedMethodException(T4Properties t4props, Locale locale, String s) throws TrafT4Exception {
        Object[] messageArguments = new Object[1];

        messageArguments[0] = s;
        throw TrafT4Messages.createSQLException(t4props, "deprecated_method", messageArguments);
    } // end throwDeprecatedMethodException

} // end class TrafT4Messages
