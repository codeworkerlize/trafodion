package com.esgyn.rs;

import java.net.ConnectException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import org.apache.hadoop.hbase.coprocessor.transactional.lock.LockConstants;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.message.LMLockInfoMessage;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.message.LMLockInfoReqMessage;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.message.LMLockMemInfoMessage;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.message.LMLockMsg;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.message.LMLockStaticisticsData;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.utils.StringUtil;
import org.apache.hadoop.hbase.coprocessor.transactional.message.*;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.message.LMLockStatisticsMessage;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.message.LMLockStatisticsReqMessage;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.message.LMMRegionInfoMsg;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.message.TransactionMsg;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.message.LMAppliedLockMessage;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.message.LMAppliedLockReqMessage;
import org.apache.hadoop.hbase.coprocessor.transactional.server.RSConnection;
import org.apache.hadoop.hbase.coprocessor.transactional.lock.utils.LockUtils;
import org.json.JSONObject;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.IStringConverter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.validators.PositiveInteger;

public class RSciCommand {

    //only use for parameter check error message
    public static String parameterCheckErrorMessage;

    //only for parameter check error
    private class parameterCheckException extends Exception {
        private static final long serialVersionUID = 1L;

        public parameterCheckException(String msg) {
            super(msg);
        }
    }

    @Parameter(names = {"-host", "-h"}, help = true, required = false, description = "hostname or ip", order = 1)
    private String host = "localhost";

    @Parameter(names = {"-port", "-p"}, help = true, required = false, description = "port", order = 2)
    private int port = 8888;

    @Parameter(names = {"-type", "-t"}, help = true, required = false, converter = RSciCommandToUpperConverter.class, validateWith = RSciCommandTypeValidator.class,
            description = "LOCK_AND_WAIT | ALL_LOCK | LOCK_WAIT | LOCK_CLEAN | DEAD_LOCK | LOCK_STATISTICS | LOCK_INFO | RS_PARAMETER | LOCK_PARAMETER | GET_PARAMETER | GET_LOCK_PARAMETER | GET_RS_PARAMETER | LOCK_STATUS | LOCK_CHECK | LOCK_COUNT_DOWN | LOCK_APPLY | LOCK_MEM", order = 3)
    private static String type = RSMessage.LOCK_STATISTICS;

    public static class RSciCommandToUpperConverter implements IStringConverter<String> {
        @Override
        public String convert(String value) {
            return value.toUpperCase();
        }

    }

    public static class RSciCommandTypeValidator implements IParameterValidator {
        List<String> types = Arrays.asList(RSMessage.LOCK_AND_WAIT, "ALL_LOCK", RSMessage.LOCK_WAIT,
                RSMessage.LOCK_CLEAN, RSMessage.DEAD_LOCK, RSMessage.LOCK_STATISTICS, RSMessage.LOCK_INFO,
                RSMessage.RS_PARAMETER, RSMessage.LOCK_PARAMETER, RSMessage.GET_PARAMETER, RSMessage.GET_LOCK_PARAMETER,
                RSMessage.GET_RS_PARAMETER, RSMessage.LOCK_STATUS,
                RSMessage.LOCK_CHECK, "LOCK_COUNT_DOWN", "LOCK_APPLY", RSMessage.LOCK_MEM);

        @Override
        public void validate(String name, String value) throws ParameterException {
            boolean inflag = true;
            for (String type : types) {
                if (type.equals(value.toUpperCase())) {
                    inflag = false;
                    break;
                }
            }
            if (inflag) {
                StringBuffer messageBuf = new StringBuffer("Parameter " + name + " should be one of ");
                for (String type : types) {
                    messageBuf.append(type).append(" | ");
                }
                messageBuf.delete(messageBuf.length() - 2, messageBuf.length());
                messageBuf.append("(found ").append(value).append(")");
                throw new ParameterException(messageBuf.toString());
            }
        }
    }

    @Parameter(names = {
            "-table"}, help = true, required = false, description = "table name, split by \",\"", order = 4)
    private List<String> tableNames = new ArrayList<>();

    @Parameter(names = {
            "-tx"}, help = true, required = false, description = "transaction ID, split by \",\"", order = 5)
    private List<Long> txIDs = new ArrayList<>();

    @Parameter(names = {
            "-htx"}, help = true, required = false, description = "hold transaction ID, split by \",\"", hidden = true)
    private List<Long> holderTxIDs = new ArrayList<>();

    @Parameter(names = {"-top"}, help = true, required = false, description = "topN", order = 6)
    private int topN = 0;

    @Parameter(names = {"-output",
            "-o"}, help = true, required = false, validateWith = RSciCommandOutputValidator.class, converter = RSciCommandToUpperConverter.class, description = "output format, just support json", order = 7)
    private String output = "TEXT";

    public static class RSciCommandOutputValidator implements IParameterValidator {
        @Override
        public void validate(String name, String value) throws ParameterException {
            if (value.toUpperCase().equals("JSON") == false) {
                throw new ParameterException("Parameter " + name + " just support json " + "(found " + value + ")");
            }
        }
    }

    @DynamicParameter(names = {"-colwidth",
            "-col"}, assignment = ",", validateWith = RSciCommandValueValidator.class, required = false, description = "set output column width, eg: \"-col 2,200 -col 3,300 -col 4,400\"", order = 8)
    private Map<String, String> colWidths = new HashMap<>();

    public static class RSciCommandValueValidator implements IParameterValidator {
        @Override
        public void validate(String name, String value) throws ParameterException {
            String[] colWidthArray = ((String) value).split(",");
            int n1 = 0, n2 = 0;
            try {
                n1 = Integer.parseInt(colWidthArray[0]);
                n2 = Integer.parseInt(colWidthArray[1]);
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new ParameterException(
                        "value of -colwidth or -col is match \"-col[width] int,int\" " + "(found " + value + ")");
            }
            if (n1 <= 0 || n1 > 7) {
                throw new ParameterException("Parameter " + name + " should be between 1 and 7 (found " + n1 + ")");
            }
            if (n2 <= 0 || n2 > 2000) {
                throw new ParameterException("Parameter " + name + " should be between 1 and 2000 (found " + n2 + ")");
            }
        }
    }

    @Parameter(names = {"-removespace",
            "-r"}, help = true, required = false, description = "remove all 20 from rowid", order = 9)
    private boolean removespace = false;

    @Parameter(names = {"-debug",
            "-d"}, help = false, validateWith = RSciCommandDebugValidator.class, required = false, description = "debug 0 or 1", order = 10)
    private int debug = 0;

    public static class RSciCommandDebugValidator implements IParameterValidator {
        @Override
        public void validate(String name, String value) throws ParameterException {
            int n1 = Integer.parseInt(value);
            if ((n1 == 1 || n1 == 0) == false) {
                throw new ParameterException("Parameter " + name + " debug is 0 or 1 " + "(found " + value + ")");
            }
        }
    }

    @Parameter(names = {"-merge",
            "-m"}, help = true, required = false, validateWith = RSciCommandMergeModeValidator.class, converter = RSciCommandToUpperConverter.class, description = "[local|server], do merge on server or local default is local", order = 11)
    private String mergeMode = "";

    public static class RSciCommandMergeModeValidator implements IParameterValidator {
        @Override
        public void validate(String name, String value) throws ParameterException {
            if ((value.toUpperCase().equals("LOCAL") || value.toUpperCase().equals("SERVER")) == false) {
                throw new ParameterException(
                        "Parameter " + name + " should be one of local or server " + "(found " + value + ")");
            }
        }
    }

    @Parameter(names = {"-timeout",
            "-to"}, help = true, required = false, validateWith = PositiveInteger.class, description = "timeout, for wait for server returned lock info data. default is 60s.", order = 12)
    private int waitTimeoutNum = 60;

    @Parameter(names = {"-displayimplictit",
            "-di"}, help = true, required = false, description = "display mark for implictit savepoint on column [lock mode]. default is false", order = 13)
    private boolean displayimplictitsavepoint = false;

    @Parameter(names = {"-displaysavepointid",
            "-dsi"}, help = true, required = false, description = "display savepoint id, default is false", order = 14)
    private boolean displaysavepointid = false;

    @Parameter(names = {"-configfile",
            "-cf"}, help = true, required = false, description = "rs parameters in config file to set in hbase, default is $TRAFCONF/trafodion.lock.config only work with -type RS_PARAMETER|LOCK_PARAMETER, eg: rsci -t LOCK_PARAMETER -cf file ", order = 15)
    private String configfile = "";

    @DynamicParameter(names = {"-confpara",
            "-cp"}, required = false, validateWith = RSParameterConfParaValidator.class, description = "lock parameters to set in hbase, eg:\"-cp p1=v1 -cp p2=v2\" only work with -type RS_PARAMETER|LOCK_PARAMETER, eg: rsci -t LOCK_PARAMETER -cp REST_READ_TIMEOUT=30000", order = 16)
    private Map<String, String> configparas = new HashMap<>();

    @Parameter(names = { "-memoryunit",
            "-mu" }, help = true, required = false, validateWith = RSciCommandMemUnitParaValidator.class, description = "the unit of memory for display lock memory useage.default is K.only work with -t LOCK_MEM , eg: rsci -t LOCK_MEM -mu [K|M|G] .", order = 17)
    private String memUnit = "";

    @Parameter(names = { "-showcleanCommand",
            "-sc" }, help = true, required = false, description = "show command to clean the lock. ", order = 18)
    private  boolean showcleanCommand = false;

    @Parameter(names = { "-maxlockinfo",
    "-max" }, help = true, required = false, validateWith = RSciCommandMaxRetLockInfoLimitValidator.class, description = "set max number of lock information. only work with -type LOCK_INFO|LOCK_AND_WAIT|ALL_LOCK. default is server to determine the max number.", order = 19)
    private int maxRetLockInfoLimit = -1;

    public static class RSciCommandMaxRetLockInfoLimitValidator implements IParameterValidator {
        @Override
        public void validate(String name, String value) throws ParameterException {
            try{
                int num = Integer.parseInt(value);
                if (num <= 0) {
                    throw new Exception();
                }
            } catch (Exception e) {
                throw new ParameterException("Parameter " + name + " must be an integer and greater than zero.");
            }
        }
    }

    public static class RSciCommandMemUnitParaValidator implements IParameterValidator {
        @Override
        public void validate(String name, String value) throws ParameterException {
            if (value.equalsIgnoreCase("k")) {
            } else if (value.equalsIgnoreCase("m")) {
            } else if (value.equalsIgnoreCase("g")) {
            } else {
                throw new ParameterException(
                        "Parameter " + name + " should be one of [K,M,G]" + " (found " + value + ")");
            }
        }
    }

    public static class RSParameterConfParaValidator implements IParameterValidator {
        @Override
        public void validate(String name, String value) throws ParameterException {
            List<String> entries = null;
            if (RSciCommand.type.equals(RSMessage.LOCK_PARAMETER)) {
                entries = RSciParameterCheck.LOCK_PARAMETERS;
            } else {
                entries = RSciParameterCheck.RS_PARAMETERS;
            }
            String[] configps = ((String) value).split("=");
            String key1 = null, value1 = null;
            try {
                key1 = configps[0];
                value1 = configps[1];
            } catch (ArrayIndexOutOfBoundsException e) {
                throw new ParameterException("value of -configpara or -cp is match \"-configpara|cp  string=string\" "
                        + "(found " + value + ")");
            }
            boolean inflag = true;
            for (String entry : entries) {
                if (entry.equals(key1.trim().toUpperCase())) {
                    inflag = false;
                    break;
                }
            }
            if (inflag) {
                StringBuffer sb = new StringBuffer(100);
                for (String str : entries)
                    sb.append(str + "\n");
                throw new ParameterException(
                        "Parameter " + name + " should be one of\n" + sb.toString() + "\n(found " + key1 + ")");
            }
        }
    }

    @Parameter(names = "-shownonwaiting", help = true, description = "show waiting and non waiting transactions, and holdertxid of not waiting' is -1", order = 20)
    private boolean shownonwaiting = false;

    @Parameter(names = "-force", help = true, description = "force do clean locks or set parameter", order = 21)
    private boolean forceFlag = false;

    @Parameter(names = "-help", help = true, description = "help information.", order = 22)
    private boolean help;

    public void commandDeal(String[] args) {
        com.esgyn.rs.RSciCommand main = new com.esgyn.rs.RSciCommand();
        JCommander jct = JCommander.newBuilder().addObject(main).build();
        jct.setProgramName("rsci");
        jct.setCaseSensitiveOptions(false);
        try {
            jct.parse(args);
            if (main.help) {
                jct.usage();
                return;
            }
            main.run();
        } catch (Exception ex) {
            // 为了方便使用，同时输出exception的message
            if (output.equals("JSON")) {
                System.out.println("{\"errorMessage\":\"" + ex.getMessage() + "\"}");
                System.out.println("{}");
            } else {
                System.out.println(ex.getMessage());
            }
            // jct.usage();
        }
    }

    private void InitColumnWidths() {
        if (!output.equals("JSON")) {
            // colwidth
            if (colWidths.size() != 0) {
                Iterator<Entry<String, String>> entries = colWidths.entrySet().iterator();
                while (entries.hasNext()) {
                    Entry<String, String> entry = entries.next();
                    String key = entry.getKey();
                    String value = entry.getValue();
                    LockConstants.COLUMN_WIDTH_MAP.put(Integer.parseInt(key), Integer.parseInt(value));
                }
            }
        }
    }

    public void run() {

        // Test_Base();

        RSMessage sentMessage = null;
        RSMessage receivedMessage = null;
        RSMessage responseMsg = null;
        RSConnection rsConnection = null;

        try {
            // 1. connect
            try {
                // for mantis-16657
                rsConnection = new RSConnection(host, port, true);
            } catch (Exception e) {
                throw new ConnectException(e.getMessage());
            }

            // 2.1 local merge
            if ((mergeMode.equals("") || mergeMode.equals("LOCAL")) &&
                (type.equals(RSMessage.LOCK_AND_WAIT) || type.equals("ALL_LOCK") ||
                 type.equals(RSMessage.LOCK_WAIT) || type.equals(RSMessage.DEAD_LOCK) ||
                 type.equals(RSMessage.LOCK_INFO) || type.equals(RSMessage.LOCK_CHECK) ||
                 type.equals(RSMessage.LOCK_MEM))) {
                sentMessage = RSMessage.getMessage(RSMessage.MSG_TYP_LOCK_GET_REGION_INFO);
                // sendMessage and receiveMesage
                try {
                    // for mantis-16657
                    rsConnection.send(sentMessage);
                    responseMsg = rsConnection.receive_timeout(waitTimeoutNum * 1000);
                } catch (Exception e) {
                    throw new ConnectException(e.getMessage());
                }

                receivedMessage = RSMessage.getMessage(RSMessage.MSG_TYP_LOCK_INFO_RESPONSE);

                if (responseMsg.getMsgType() != RSMessage.MSG_TYP_LOCK_GET_REGION_INFO) {
                    throw new Exception("unexpected lock response message: " + responseMsg + " from " + host);
                }
                List<String> regionList = ((LMMRegionInfoMsg) responseMsg).getRegionList();
                if (regionList.isEmpty()) {
                    throw new Exception("not find any vaild region.");
                }

                if (type.equals(RSMessage.LOCK_STATISTICS)) {
                    sentMessage = Lock_Statistics_Pack(RSMessage.MSG_TYP_LOCK_INFO_SINGLE_NODE);
                } else if (type.equals(RSMessage.LOCK_INFO)) {
                    sentMessage = Lock_Info_Pack(RSMessage.MSG_TYP_LOCK_INFO_SINGLE_NODE);
                } else if (type.equals(RSMessage.LOCK_WAIT) || type.equals(RSMessage.DEAD_LOCK)) {
                    sentMessage = Lock_Wait_Pack(RSMessage.MSG_TYP_LOCK_INFO_SINGLE_NODE);
                } else if (type.equals(RSMessage.LOCK_AND_WAIT)) {
                    sentMessage = Lock_And_Wait_Pack(RSMessage.MSG_TYP_LOCK_INFO_SINGLE_NODE, RSMessage.LOCK_AND_WAIT);
                } else if (type.equals("ALL_LOCK")) {
                    sentMessage = Lock_And_Wait_Pack(RSMessage.MSG_TYP_LOCK_INFO_SINGLE_NODE, RSMessage.LOCK_AND_WAIT);
                } else if (type.equals(RSMessage.LOCK_CHECK)) {
                    sentMessage = Lock_Check_Pack(RSMessage.MSG_TYP_LOCK_INFO_SINGLE_NODE);
                } else if (type.equals(RSMessage.LOCK_MEM)) {
                    sentMessage = Lock_Mem_Pack(RSMessage.MSG_TYP_LOCK_MEM_SINGLE_NODE_INFO);
                    receivedMessage = RSMessage.getMessage(RSMessage.MSG_TYP_LOCK_MEM_INFO_RESPONSE);
                }else {
                    throw new parameterCheckException("-merge local or -m local doesn't matchs " + type);
                }
                if (sentMessage == null) {
                    throw new parameterCheckException(parameterCheckErrorMessage);
                }

                this.sendAndReceiveFromAllServers(regionList, sentMessage, receivedMessage);
                responseMsg = receivedMessage;
            } else {
                // 2.2 server merge
                if (mergeMode.equals("LOCAL")) {
                    throw new parameterCheckException("-merge local or -m local doesn't matchs " + type);
                }
                // pack requestMessage
                if (type.equals(RSMessage.LOCK_STATISTICS)) {
                    sentMessage = Lock_Statistics_Pack(RSMessage.MSG_TYP_LOCK_STATISTICS_INFO);
                } else if (type.equals(RSMessage.LOCK_INFO)) {
                    sentMessage = Lock_Info_Pack(RSMessage.MSG_TYP_LOCK_INFO);
                } else if (type.equals(RSMessage.LOCK_WAIT) || type.equals(RSMessage.DEAD_LOCK)) {
                    sentMessage = Lock_Wait_Pack(RSMessage.MSG_TYP_LOCK_INFO);
                } else if (type.equals(RSMessage.LOCK_AND_WAIT)) {
                    sentMessage = Lock_And_Wait_Pack(RSMessage.MSG_TYP_LOCK_INFO, RSMessage.LOCK_AND_WAIT);
                } else if (type.equals("ALL_LOCK")) {
                    sentMessage = Lock_And_Wait_Pack(RSMessage.MSG_TYP_LOCK_INFO, RSMessage.LOCK_AND_WAIT);
                } else if (type.equals(RSMessage.LOCK_CLEAN)) {
                    sentMessage = Lock_Clean_Pack(RSMessage.MSG_TYP_LOCK_CLEAN);
                } else if (type.equals(RSMessage.GET_PARAMETER) || type.equals(RSMessage.GET_RS_PARAMETER) || type.equals(RSMessage.GET_LOCK_PARAMETER)) {
                    sentMessage = GET_PARAMETER_Pack(RSMessage.MSG_TYP_GET_RS_PARAMETER, type);
                } else if (type.equals(RSMessage.LOCK_STATUS)) {
                    sentMessage = GET_PARAMETER_Pack(RSMessage.MSG_TYP_GET_RS_PARAMETER, type);
                } else if (type.equals("LOCK_APPLY")) {
                    sentMessage = LOCK_APPLY_Pack(RSMessage.MSG_TYP_LOCK_APPLIED_LOCK_MSG);
                } else if (type.equals("LOCK_COUNT_DOWN")) {
                    sentMessage = LOCK_COUNT_DOWN_Pack(RSMessage.MSG_TYP_LOCK_CLEAN_LOCK_COUNTER);
                } else if (type.equals(RSMessage.RS_PARAMETER) || type.equals(RSMessage.LOCK_PARAMETER)) {
                    if (configfile.equals("") && configparas.size() != 0) {
                        sentMessage = RS_Parameter_Pack();
                    } else if ((configfile.equals("") == false) && configparas.size() == 0) {
                        sentMessage = RS_Parameter_File_Pack();
                    } else {
                        sentMessage = null;
                        parameterCheckErrorMessage = "only support option: \"-confpara -cp \"";
                    }
                } else if (type.equals(RSMessage.LOCK_MEM)) {
                    sentMessage = Lock_Mem_Pack(RSMessage.MSG_TYP_LOCK_MEM_INFO);
                } else {
                    throw new parameterCheckException("not support option: " + type);
                }
                if (sentMessage == null) {
                    throw new parameterCheckException(parameterCheckErrorMessage);
                }
                // sendMessage and receiveMesage
                try {
                    // for mantis-16657
                    rsConnection.send(sentMessage);
                    responseMsg = rsConnection.receive_timeout(waitTimeoutNum * 1000);
                } catch (Exception e) {
                    String errMsg = e.getMessage();
                    if (errMsg.startsWith("failed to receive message from")) {
                        //not ConnectException
                        throw new SocketTimeoutException(errMsg);
                    } else {
                        throw new ConnectException(errMsg);
                    }
                }
            }
            // 3. unpack resonMessage
            if (type.equals(RSMessage.LOCK_STATISTICS)) {
                Lock_Statistics_Unpack(responseMsg);
            } else if (type.equals(RSMessage.LOCK_WAIT) || type.equals(RSMessage.DEAD_LOCK)) {
                Lock_Wait_Dead_Unpack(responseMsg);
            } else if (type.equals(RSMessage.LOCK_INFO)) {
                Lock_Info_Unpack(responseMsg);
            } else if (type.equals("ALL_LOCK")) {
                All_Lock_Unpack(responseMsg);
            } else if (type.equals("LOCK_AND_WAIT")) {
                Lock_And_Wait_Unpack(responseMsg);
            } else if (type.equals("LOCK_COUNT_DOWN")) {
                LOCK_COUNT_DOWN_Unpack(responseMsg);
            } else if (type.equals("LOCK_APPLY")) {
                LOCK_APPLY_Unpack(responseMsg);
            } else if (type.equals(RSMessage.LOCK_CLEAN)) {
                if (responseMsg.getMsgType() == RSMessage.MSG_TYP_OK) {
                    System.out.println("lock clean success");
                } else {
                    System.out.println("failed to clean lock");
                }
            } else if (type.equals(RSMessage.GET_PARAMETER) || type.equals(RSMessage.GET_RS_PARAMETER) || type.equals(RSMessage.GET_LOCK_PARAMETER)) {
                Get_Parameter_Unpack(responseMsg);
            } else if (type.equals(RSMessage.LOCK_STATUS)) {
                Lock_Status_Unpack(responseMsg);
            } else if (type.equals(RSMessage.RS_PARAMETER) || type.equals(RSMessage.LOCK_PARAMETER)) {
                if (responseMsg.getMsgType() == RSMessage.MSG_TYP_OK) {
                    System.out.println("set lock parameter success");
                } else {
                    System.out.println(responseMsg.getErrorMsg());
                    System.out.println("failed to set lock parameter");
                }
            } else if (type.equals(RSMessage.LOCK_CHECK)) {
                Lock_Check_Unpack(responseMsg);
            } else if (type.equals(RSMessage.LOCK_MEM)) {
                LOCK_Mem_Unpack(responseMsg);
            } else {
                throw new RuntimeException("no support type : " + type);
            }

        } catch (ConnectException e) {
            String msg = "can not connect server, pleasse check the port " + port + " on " + host
                    + ", please check if the region server is on and there are regions on this region server";
            if (output.equals("JSON")) {
                System.out.println("{\"errorMessage\":\"" + msg + "\"}");
                System.out.println("{}");
            } else {
                System.out.println(msg);
            }
        } catch (parameterCheckException e) {
            if (output.equals("JSON")) {
                System.out.println("{\"errorMessage\":\"" + e.getMessage() + "\"}");
                System.out.println("{}");
            } else {
                System.out.println(e.getMessage());
            }
        } catch (SocketTimeoutException e) {
            String errMsg = e.getMessage();
            if (output.equals("JSON")) {
                errMsg += "\\\\n";
            } else {
                errMsg += "\n";
            }
            errMsg += "try specifying '-timeout' with number more then " + String.valueOf(waitTimeoutNum)
                    + " to increase the wait time of rsci.";

            if (output.equals("JSON")) {
                System.out.println("{\"errorMessage\":\"" + errMsg + "\"}");
                System.out.println("{}");
            } else {
                System.out.println(errMsg);
            }
        } catch (Exception e) {
            if (output.equals("JSON")) {
                System.out.println("{\"errorMessage\":\"" + e.getMessage() + "\"}");
                System.out.println("{}");
            } else {
                System.out.println(e.getMessage());
                e.printStackTrace();
            }
        } finally {
            if (rsConnection != null) {
                rsConnection.close();
            }
        }
    }

    private boolean parameterCheck() {
        return RSciParameterCheck.TableNamesCheck(tableNames, type) || RSciParameterCheck.TxIDsCheck(txIDs, type) || RSciParameterCheck.TxIDsValueCheck(txIDs, type) ||
                RSciParameterCheck.HolderTxIDsCheck(holderTxIDs, type) || RSciParameterCheck.DisplayImplictitSavepointCheck(displayimplictitsavepoint, type) ||
                RSciParameterCheck.DisplaySavepointIdCheck(displaysavepointid, type) || RSciParameterCheck.TopCheck(topN, type) || RSciParameterCheck.OutputCheck(output, type) ||
                RSciParameterCheck.DebugCheck(debug, type) || RSciParameterCheck.ColWidthsCheck(colWidths, type) || RSciParameterCheck.RemovespaceCheck(removespace, type) ||
                RSciParameterCheck.ConfigFileCheck(configfile, configparas, type) || RSciParameterCheck.ConfigParasCheck(configfile, configparas, type) || RSciParameterCheck.MemUnitParasCheck(memUnit, type) ||
                RSciParameterCheck.ShowNonWaitCheck(shownonwaiting, type) || RSciParameterCheck.ForceFlagCheck(forceFlag, type) ||
                RSciParameterCheck.ShowCleanCommandCheck(showcleanCommand, type) || RSciParameterCheck.MaxCheck(maxRetLockInfoLimit, type);
    }

    private RSMessage Lock_Mem_Pack(byte msg_type) {
        if (parameterCheck()) {
            return null;
        }
        return RSMessage.getMessage(msg_type);
    }

    private RSMessage Lock_Statistics_Pack(byte msg_type) {
        RSMessage sentMessage = null;
        // holderIDs
        if (parameterCheck()) {
            return sentMessage;
        }

        //set width of column
        InitColumnWidths();

        LMLockStatisticsReqMessage lmLockStatisticsReqMessage = (LMLockStatisticsReqMessage) RSMessage
                .getMessage(msg_type);
        lmLockStatisticsReqMessage.setDebug(debug);
        lmLockStatisticsReqMessage.setTopN(topN);
        lmLockStatisticsReqMessage.setTableNames(tableNames);
        lmLockStatisticsReqMessage.setTxIDs(txIDs);
        sentMessage = lmLockStatisticsReqMessage;
        return sentMessage;
    }

    private RSMessage Lock_Info_Pack(byte msg_type) {
        RSMessage sentMessage = null;
        // holderIDs
        if (parameterCheck()) {
            return sentMessage;
        }

        //set width of column
        InitColumnWidths();

        // removespace
        if (removespace) {
            LockConstants.REMOVE_SPACE_FROM_ROWID = true;
        }
        // displayimplictitsavepoint
        if (displayimplictitsavepoint) {
            LockConstants.DISPLAY_IMPLICIT_SAVEPOINT = true;
        }
        // displaysavepointed
        if (displaysavepointid) {
            LockConstants.DISPLAY_SAVEPOINT_ID = true;
        }
        LMLockInfoReqMessage lmLockInfoReqMessage = new LMLockInfoReqMessage(msg_type);
        lmLockInfoReqMessage.setType(type);
        lmLockInfoReqMessage.setTableNames(tableNames);
        lmLockInfoReqMessage.setTxIDs(txIDs);
        lmLockInfoReqMessage.setHolderTxIDs(holderTxIDs);
        lmLockInfoReqMessage.setDebug(debug);
        lmLockInfoReqMessage.setMaxRetLockInfoLimit((maxRetLockInfoLimit == -1) ? 0 : maxRetLockInfoLimit);
        sentMessage = lmLockInfoReqMessage;
        return sentMessage;
    }

    private RSMessage Lock_Wait_Pack(byte msg_type) {
        RSMessage sentMessage = null;

        if (parameterCheck()) {
            return sentMessage;
        }

        //set width of column
        InitColumnWidths();

        // removespace
        if (removespace) {
            LockConstants.REMOVE_SPACE_FROM_ROWID = true;
        }
        // shownonwaiting
        if (shownonwaiting) {
            LockConstants.SHOW_NON_WAITING = true;
        }

        LMLockInfoReqMessage lmLockInfoReqMessage = new LMLockInfoReqMessage(msg_type);
        lmLockInfoReqMessage.setType(type);
        lmLockInfoReqMessage.setTableNames(tableNames);
        lmLockInfoReqMessage.setTxIDs(txIDs);
        lmLockInfoReqMessage.setHolderTxIDs(holderTxIDs);
        lmLockInfoReqMessage.setDebug(debug);
        sentMessage = lmLockInfoReqMessage;
        return sentMessage;
    }

    private RSMessage Lock_And_Wait_Pack(byte msg_type, String new_type) {
        RSMessage sentMessage = null;
        if (parameterCheck()) {
            return sentMessage;
        }

        //set width of column
        InitColumnWidths();

        // removespace
        if (removespace) {
            LockConstants.REMOVE_SPACE_FROM_ROWID = true;
        }
        // displayimplictitsavepoint
        if (displayimplictitsavepoint) {
            LockConstants.DISPLAY_IMPLICIT_SAVEPOINT = true;
        }
        // displaysavepointed
        if (displaysavepointid) {
            LockConstants.DISPLAY_SAVEPOINT_ID = true;
        }
        // shownonwaiting
        if (shownonwaiting) {
            LockConstants.SHOW_NON_WAITING = true;
        }

        LMLockInfoReqMessage lmLockInfoReqMessage = new LMLockInfoReqMessage(msg_type);
        lmLockInfoReqMessage.setType(new_type);
        lmLockInfoReqMessage.setTableNames(tableNames);
        lmLockInfoReqMessage.setTxIDs(txIDs);
        lmLockInfoReqMessage.setHolderTxIDs(holderTxIDs);
        lmLockInfoReqMessage.setDebug(debug);
        lmLockInfoReqMessage.setMaxRetLockInfoLimit((maxRetLockInfoLimit == -1) ? 0 : maxRetLockInfoLimit);
        sentMessage = lmLockInfoReqMessage;
        return sentMessage;
    }

    private RSMessage Lock_Clean_Pack(byte msg_type) {
        RSMessage sentMessage = null;
        if (parameterCheck()) {
            return sentMessage;
        }

        LMLockInfoReqMessage lmLockInfoReqMessage = new LMLockInfoReqMessage(msg_type);
        lmLockInfoReqMessage.setType(type);
        lmLockInfoReqMessage.setTableNames(tableNames);
        lmLockInfoReqMessage.setTxIDs(txIDs);
        lmLockInfoReqMessage.setHolderTxIDs(holderTxIDs);
        lmLockInfoReqMessage.setDebug(debug);
        lmLockInfoReqMessage.setForceClean(forceFlag);
        sentMessage = lmLockInfoReqMessage;
        return sentMessage;
    }

    private RSMessage Lock_Check_Pack(byte msg_type) {
        RSMessage sentMessage = null;

        if (parameterCheck()) {
            return sentMessage;
        }

        LMLockInfoReqMessage lmLockInfoReqMessage = new LMLockInfoReqMessage(msg_type);
        lmLockInfoReqMessage.setType(RSMessage.LOCK_CHECK);
        lmLockInfoReqMessage.setTableNames(tableNames);
        lmLockInfoReqMessage.setTxIDs(txIDs);
        lmLockInfoReqMessage.setHolderTxIDs(holderTxIDs);
        lmLockInfoReqMessage.setDebug(debug);
        sentMessage = lmLockInfoReqMessage;
        return sentMessage;
    }

    private RSMessage GET_PARAMETER_Pack(byte msg_type, String new_type) {
        RSMessage sentMessage = null;
        if (parameterCheck()) {
            return sentMessage;
        }
        LMLockInfoReqMessage lmLockInfoReqMessage = new LMLockInfoReqMessage(msg_type);
        lmLockInfoReqMessage.setType(new_type);
        lmLockInfoReqMessage.setTableNames(tableNames);
        lmLockInfoReqMessage.setTxIDs(txIDs);
        lmLockInfoReqMessage.setHolderTxIDs(holderTxIDs);
        lmLockInfoReqMessage.setDebug(debug);
        sentMessage = lmLockInfoReqMessage;
        return sentMessage;
    }

    private RSMessage LOCK_APPLY_Pack(byte msg_type) {
        RSMessage sentMessage = null;
        if (parameterCheck()) {
            return sentMessage;
        }

        //set width of column
        InitColumnWidths();

        LMAppliedLockReqMessage lmAppliedLockReqMessage = (LMAppliedLockReqMessage) RSMessage.getMessage(msg_type);
        lmAppliedLockReqMessage.setDebug(debug);
        sentMessage = lmAppliedLockReqMessage;
        return sentMessage;
    }

    private RSMessage LOCK_COUNT_DOWN_Pack(byte msg_type) {
        RSMessage sentMessage = null;
        if (parameterCheck()) {
            return sentMessage;
        }
        sentMessage = RSMessage.getMessage(msg_type);
        return sentMessage;
    }

    private RSMessage RS_Parameter_Pack() {
        RSMessage sentMessage = null;
        if (parameterCheck()) {
            return sentMessage;
        }

        Properties properties = new Properties();
        if (configparas.size() != 0) {
            Iterator<Entry<String, String>> entries = configparas.entrySet().iterator();
            while (entries.hasNext()) {
                Entry<String, String> entry = entries.next();
                String key = entry.getKey();
                String value = entry.getValue();
                properties.setProperty(key.toUpperCase(), value);
            }
        }
        RSParameterReqMessage rsParameterMessage = (RSParameterReqMessage) RSMessage
                .getMessage(RSMessage.MSG_TYP_RS_PARAMETER);
        rsParameterMessage.setParameters(properties);
        rsParameterMessage.setForced(forceFlag);
        sentMessage = rsParameterMessage;
        return sentMessage;
    }

    private RSMessage RS_Parameter_File_Pack() {
        RSMessage sentMessage = null;
        if (parameterCheck()) {
            return sentMessage;
        }
        if (configfile.equals("") == false) {
            try {
                Properties properties = StringUtil.readConfigFile(configfile);

                RSParameterReqMessage lmLockParameterMessage = (RSParameterReqMessage) RSMessage
                        .getMessage(RSMessage.MSG_TYP_RS_PARAMETER);
                lmLockParameterMessage.setParameters(properties);
                lmLockParameterMessage.setForced(forceFlag);
                sentMessage = lmLockParameterMessage;
            } catch (Exception e) {
                System.out.println(e.getMessage());
                return sentMessage;
            }
        }
        return sentMessage;
    }

    private void Lock_Error_Unpack(RSMessage responseMsg, boolean outputJson) {
        if (responseMsg.isErrorFlag()) {
            if (outputJson) {
                System.out.println("{\"errorMessage\":\"" + responseMsg.getErrorMsg() + "\"}");
            } else {
                System.out.println("errorMsg:");
                System.out.println(responseMsg.getErrorMsg());
            }
        } else {
            if (outputJson) {
                System.out.println("{}");
            }
        }
    }

    private void Lock_Statistics_Unpack(RSMessage responseMsg) {
        boolean outputJson = output.equals("JSON") ? true : false;
        if (responseMsg.getMsgType() != RSMessage.MSG_TYP_LOCK_STATISTICS_INFO_RESPONSE) {
            throw new RuntimeException("failed to get lock statistics");
        } else {
            Lock_Error_Unpack(responseMsg, outputJson);
            if (outputJson) {
                System.out.println(((LMLockStatisticsMessage) responseMsg).getAllLockStatisticsJson());
            } else {
                ((LMLockStatisticsMessage) responseMsg).getAllLockStatistics();
            }
        }
    }

    private void Lock_Info_Unpack(RSMessage responseMsg) {
        boolean outputJson = output.equals("JSON") ? true : false;
        if (responseMsg.getMsgType() != RSMessage.MSG_TYP_LOCK_INFO_RESPONSE) {
            throw new RuntimeException("failed to get lock info");
        } else {
            Lock_Error_Unpack(responseMsg, outputJson);
            if (outputJson) {
                System.out.println(((LMLockInfoMessage) responseMsg).getAllLockJson());
            } else {
                System.out.println("all locks:");
                ((LMLockInfoMessage) responseMsg).getAllLock();
            }
        }
    }

    private void Lock_Wait_Dead_Unpack(RSMessage responseMsg) {
        boolean outputJson = output.equals("JSON") ? true : false;
        if (responseMsg.getMsgType() != RSMessage.MSG_TYP_LOCK_INFO_RESPONSE) {
            throw new RuntimeException("failed to get Lock Wait or Dead Lock");
        } else {
            Lock_Error_Unpack(responseMsg, outputJson);
            if (outputJson) {
                System.out.println(((LMLockInfoMessage) responseMsg).getLockWaitJson());
            } else {
                System.out.println("lock wait:");
                ((LMLockInfoMessage) responseMsg).getLockWait();
            }
        }
    }

    private void Lock_And_Wait_Unpack(RSMessage responseMsg) {
        boolean outputJson = output.equals("JSON") ? true : false;
        if (responseMsg.getMsgType() != RSMessage.MSG_TYP_LOCK_INFO_RESPONSE) {
            throw new RuntimeException("failed to get Lock Wait or Dead Lock");
        } else {
            Lock_Error_Unpack(responseMsg, outputJson);
            if (output.equals("JSON")) {
                String waitJsonStr = ((LMLockInfoMessage) responseMsg).getLockWaitJson();
                String infoJsonStr = ((LMLockInfoMessage) responseMsg).getAllLockJson();
                System.out.println(waitJsonStr);
                System.out.println(infoJsonStr);
            } else {
                System.out.println("lock wait:");
                ((LMLockInfoMessage) responseMsg).getLockWait();
                System.out.println("all locks:");
                ((LMLockInfoMessage) responseMsg).getAllLock();
            }
        }
    }

    private void LOCK_COUNT_DOWN_Unpack(RSMessage responseMsg) {
        System.out.println("OK");
    }

    private void LOCK_APPLY_Unpack(RSMessage responseMsg) {
        if (responseMsg.getMsgType() != RSMessage.MSG_TYP_LOCK_APPLIED_LOCK_RESPONSE) {
            throw new RuntimeException("failed to get Lock apply info");
        } else {
            //-o json is not supported now
            if (colWidths.isEmpty()) {
                if (debug != 0) {
                    LockConstants.COLUMN_WIDTH_MAP.put(1, 19);
                    LockConstants.COLUMN_WIDTH_MAP.put(2, 25);
                    LockConstants.COLUMN_WIDTH_MAP.put(3, 30);
                    LockConstants.COLUMN_WIDTH_MAP.put(4, 19);
                } else {
                    LockConstants.COLUMN_WIDTH_MAP.put(1, 19);
                    LockConstants.COLUMN_WIDTH_MAP.put(2, 19);
                }
            }
            ((LMAppliedLockMessage) responseMsg).display();
        }
    }

    private void LOCK_Mem_Unpack(RSMessage responseMsg) {
        if (responseMsg.getMsgType() != RSMessage.MSG_TYP_LOCK_MEM_INFO_RESPONSE) {
            throw new RuntimeException("failed to get the mem useage of Lock");
        } else {
            Lock_Error_Unpack(responseMsg, output.equals("JSON"));
            LMLockMemInfoMessage.MEMORYUNIT mu = LMLockMemInfoMessage.MEMORYUNIT.KB;
            if (!memUnit.isEmpty()) {
                if (memUnit.equalsIgnoreCase("m")) {
                    mu = LMLockMemInfoMessage.MEMORYUNIT.MB;
                } else if (memUnit.equalsIgnoreCase("g")) {
                    mu = LMLockMemInfoMessage.MEMORYUNIT.GB;
                }
            }
            LMLockMemInfoMessage memInfo = (LMLockMemInfoMessage) responseMsg;
            memInfo.setMemunit(mu);
            if (output.equals("JSON")) {
                System.out.println(memInfo.getMemInfoJson());
            } else {
                if (colWidths.isEmpty()) {
                    LockConstants.COLUMN_WIDTH_MAP.put(1, 19);
                    LockConstants.COLUMN_WIDTH_MAP.put(2, 17);
                    LockConstants.COLUMN_WIDTH_MAP.put(3, 13);
                    LockConstants.COLUMN_WIDTH_MAP.put(4, 3);
                    LockConstants.COLUMN_WIDTH_MAP.put(5, 19);
                    LockConstants.COLUMN_WIDTH_MAP.put(6, 19);
                    LockConstants.COLUMN_WIDTH_MAP.put(7, 19);
                    LockConstants.COLUMN_WIDTH_MAP.put(8, 19);
                }
                memInfo.getMemInfo();
            }
        }
    }

    private void All_Lock_Unpack(RSMessage responseMsg) {
        boolean outputJson = output.equals("JSON") ? true : false;
        if (responseMsg.getMsgType() != RSMessage.MSG_TYP_LOCK_INFO_RESPONSE) {
            throw new RuntimeException("failed to get All Lock");
        } else {
            Lock_Error_Unpack(responseMsg, outputJson);
            String lockInfo = null;
            String lockWait = null;
            if (output.equals("JSON")) {
                lockWait = ((LMLockInfoMessage) responseMsg).getLockWaitJson();
                lockInfo = ((LMLockInfoMessage) responseMsg).getAllLockJson();
            } else {
                System.out.println("lock wait:");
                ((LMLockInfoMessage) responseMsg).getLockWait();
                System.out.println("all locks:");
                ((LMLockInfoMessage) responseMsg).getAllLock();
            }
            LMLockStatisticsMessage statsInfo = (LMLockStatisticsMessage) RSMessage
                    .getMessage(RSMessage.MSG_TYP_LOCK_STATISTICS_INFO_RESPONSE);
            getStatisticFromLockInfoMessage((LMLockInfoMessage) responseMsg, statsInfo, topN);
            if (outputJson) {
                //{"lockInfo":{},"errorMessage":"","lockStats":{"summary":{},"detail":{}},"lockWait":{}}
                //json.put("lockInfo", new String("abc")); -> {"lockInfo":"{}"}
                //json.put("lockInfo", new JSONObject("abc")); -> {"lockInfo":{}}
                JSONObject json = new JSONObject();
                String errorMessage = responseMsg.getErrorMsg();
                json.put("lockInfo", new JSONObject(lockInfo));
                json.put("errorMessage", (errorMessage == null) ? "" : errorMessage);
                json.put("lockStats", new JSONObject(statsInfo.getAllLockStatisticsJson()));
                json.put("lockWait", new JSONObject(lockWait));
                System.out.println(json.toString());
            } else {
                System.out.println("lock statistics:");
                statsInfo.getAllLockStatistics();
            }
        }
    }

    private void Get_Parameter_Unpack(RSMessage responseMsg) {
        boolean outputJson = output.equals("JSON") ? true : false;
        if (responseMsg.getMsgType() != RSMessage.MSG_TYP_GET_RS_PARAMETER_RESPONSE) {
            throw new RuntimeException("failed to Get Parameter");
        } else {
            Lock_Error_Unpack(responseMsg, outputJson);
            RSParameterMessage parameterMsg = (RSParameterMessage) responseMsg;
            if (output.equals("JSON")) {
                parameterMsg.getAllParametersJson(false);
            } else {
                parameterMsg.displayAllParameters(false);
            }
        }
    }

    private void Lock_Status_Unpack(RSMessage responseMsg) {
        boolean outputJson = output.equals("JSON") ? true : false;
        if (responseMsg.getMsgType() != RSMessage.MSG_TYP_GET_RS_PARAMETER_RESPONSE) {
            throw new RuntimeException("failed to get Lock Status");
        } else {
            Lock_Error_Unpack(responseMsg, outputJson);
            RSParameterMessage parameterMsg = (RSParameterMessage) responseMsg;
            if (output.equals("JSON")) {
                parameterMsg.getAllParametersJson(true);
            } else {
                parameterMsg.displayAllParameters(true);
            }
        }
    }

    private void Lock_Check_Unpack(RSMessage responseMsg) {
        boolean outputJson = output.equals("JSON") ? true : false;
        if (responseMsg.getMsgType() != RSMessage.MSG_TYP_LOCK_INFO_RESPONSE) {
            throw new RuntimeException("failed to get check lock info");
        } else {
            Lock_Error_Unpack(responseMsg, outputJson);
            if (outputJson) {
                System.out.println("{\"errorMessage\": \"not support json format now.\"}");
                System.out.println("{}");
            } else {
                System.out.println("all check locks:");
                List<Long> transIDs = ((LMLockInfoMessage) responseMsg).getTransIDsList();
                if (transIDs != null && transIDs.size() > 0) {
                    System.out.println("transIDs size is :" + transIDs.size());
                } else {
                    System.out.println("transIDs is null");
                    return;
                }
                ((LMLockInfoMessage) responseMsg).getCheckTransID();
                if (showcleanCommand) {
                    List<Long> commitPendingTxs = ((LMLockInfoMessage) responseMsg).getCommitPendingIDs();
                    transIDs.removeAll(commitPendingTxs);
                    if (transIDs.size() == 0) {
                        System.out.println("no transaction to clean up");
                        return;
                    }
                    System.out.println("you can execute this command clean the lock: ");

                    StringBuilder tmp = new StringBuilder();
                    for(int i = 0; i < transIDs.size(); i++) {
                        tmp.append(transIDs.get(i) + ",");
                    }
                    tmp.deleteCharAt(tmp.length() - 1);
                    
                    System.out.println("rsci -t LOCK_CLEAN -tx " + tmp);
                }
            }
        }
    }

    private void sendAndReceiveFromAllServers(List<String> serverList, RSMessage sentMsg, RSMessage receivedMsg) {
        String breakLine = output.equals("JSON") ? "\\\\n" : "\n";
        StringBuffer sb = new StringBuffer();
        int serverNum = serverList.size();
        List<Thread> threadList = new ArrayList<>(serverNum);
        final RSMessage receivedMsgs[] = new RSMessage[serverNum];
        final CountDownLatch wakeLatch = new CountDownLatch(serverNum);
        final int timeout_ = waitTimeoutNum * 1000;
        final int port_ = port;
        final RSMessage sentMsg_ = sentMsg;
        final byte msgType_ = receivedMsg.getMsgType();
        for (int i = 0; i < serverNum; i++) {
            // local variable xxx is accessed from within inner class; needs to be declared
            // final
            final String host_ = serverList.get(i);
            final int i_ = i;
            Thread takeThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    RSConnection conn = null;
                    try {
                        conn = new RSConnection(host_, port_, true);
                        conn.send(sentMsg_);
                        // using receive() will be hang when region server also in hang
                        receivedMsgs[i_] = conn.receive_timeout(timeout_);
                        if (receivedMsgs[i_].getMsgType() != msgType_) {
                            throw new Exception(
                                    "unexpected lock response message: " + receivedMsgs[i_] + " from " + host_);
                        }
                    } catch (Exception e) {
                        receivedMsgs[i_] = RSMessage.getMessage(RSMessage.MSG_TYP_NO);
                        receivedMsgs[i_].setErrorFlag(true);
                        receivedMsgs[i_].setErrorMsg(e.getMessage());
                    } finally {
                        if (conn != null) {
                            conn.close();
                        }
                        wakeLatch.countDown();
                    }
                }
            });
            threadList.add(takeThread);
            takeThread.setDaemon(true);
            takeThread.start();
        }
        try {
            // wakeLatch.await(waitTimeoutNum, TimeUnit.MINUTES);
            wakeLatch.await();
        } catch (Exception e) {
            sb.append(e.getMessage());
            if (!output.equals("JSON")) {
                e.printStackTrace();
            }
            sb.append(breakLine);
        } finally {
            // kill all threads
            for (Thread t : threadList) {
                t.stop();
                t = null;
            }
        }
        boolean someoneIsTimeout = false;
        for (int i = 0; i < serverNum; i++) {
            if (receivedMsgs[i].isErrorFlag()) {
                String errMsg = receivedMsgs[i].getErrorMsg();
                if (errMsg.startsWith("failed to receive message from")) {
                    someoneIsTimeout = true;
                }
                sb.append(errMsg);
                // ignore last of \n
                if (i != serverNum - 1) {
                    sb.append(breakLine);
                }
            }
            if (receivedMsgs[i].getMsgType() != RSMessage.MSG_TYP_NO) {
                receivedMsg.merge(receivedMsgs[i]);
            }
            receivedMsgs[i] = null;
        }
        if (sb.length() != 0) {
            receivedMsg.setErrorFlag(true);
            String errMsg = sb.toString();
            if (someoneIsTimeout) {
                errMsg += breakLine;
                errMsg += "try specifying '-timeout' with number more then " + String.valueOf(waitTimeoutNum)
                        + " to increase the wait time of rsci.";
            }
            receivedMsg.setErrorMsg(errMsg);
        }
    }

    private void getStatisticFromLockInfoMessage(LMLockInfoMessage lockInfo, LMLockStatisticsMessage statsInfo,
                                                 int topN) {
        ConcurrentHashMap<Long, ConcurrentHashMap<String, Integer>> txLockNumMapMap = new ConcurrentHashMap<>();
        LinkedHashMap<Long, LMLockStaticisticsData> txLockNumMap = new LinkedHashMap<>();
        if (lockInfo.getTransactionMsgMap() == null) {
            return;
        }
        for (Map.Entry<Long, TransactionMsg> entry : lockInfo.getTransactionMsgMap().entrySet()) {
            // txID
            Long txid = entry.getKey();
            int sumLockNum = 0;
            ConcurrentHashMap<String, Integer> lockMap = new ConcurrentHashMap<>();
            // row lock
            for (Map.Entry<String, Map<String, LMLockMsg>> rowLocks : entry.getValue().getRowLocks().entrySet()) {
                // tableFullname = regionName,tableName
                String tableFullname = rowLocks.getKey();
                int lockNum = 0;
                for(LMLockMsg lockmsg : rowLocks.getValue().values()) {
                    lockNum += lockmsg.getLockNum();
                }
                lockMap.put(tableFullname, Integer.valueOf(lockNum));
                sumLockNum += lockNum;
            }
            // table lock
            Map<String, LMLockMsg> tableLocks = entry.getValue().getTableLocks();
            for (Map.Entry<String, LMLockMsg> tableLock : tableLocks.entrySet()) {
                // tableFullname = regionName,tableName
                String tableFullname = tableLock.getKey();
                if (tableFullname.isEmpty()) {
                    continue;
                }
                int tableLockNum = tableLock.getValue().getLockNum();
                if (lockMap.containsKey(tableFullname)) {
                    Integer lockNum = lockMap.get(tableFullname);
                    lockMap.replace(tableFullname, lockNum + tableLockNum);
                } else {
                    lockMap.put(tableFullname, tableLockNum);
                }
                sumLockNum += tableLockNum;
            }
            txLockNumMapMap.put(txid, lockMap);
            txLockNumMap.put(txid, new LMLockStaticisticsData(sumLockNum, entry.getValue().getQueryContext()));
        }
        if (topN > 0) {
            LinkedHashMap<Long, LMLockStaticisticsData> sortedTxLockNumMap = LockUtils.sortMapByValue(txLockNumMap, topN);
            ConcurrentHashMap<Long, ConcurrentHashMap<String, Integer>> sortedTxLockNumMapMap = new ConcurrentHashMap<Long, ConcurrentHashMap<String, Integer>>();
            {
                for (Long txId : sortedTxLockNumMap.keySet()) {
                    sortedTxLockNumMapMap.put(txId, txLockNumMapMap.get(txId));
                }
            }
            txLockNumMapMap = sortedTxLockNumMapMap;
            txLockNumMap = sortedTxLockNumMap;
        }
        statsInfo.setTxLockNumMap(new ConcurrentHashMap<Long, LMLockStaticisticsData>(txLockNumMap));
        statsInfo.setTxLockNumMapMap(txLockNumMapMap);
    }

    private void Test_Base() {
        int index = 0;
        System.out.println("host: " + host);
        System.out.println("port: " + port);
        System.out.println("type: " + type);
        System.out.println("--------------tableNames begin-------------------");
        if (tableNames != null) {
            index = 0;
            for (String table : tableNames) {
                System.out.println("tables[" + index + "]: " + table);
                index++;
            }
        }
        System.out.println("--------------tables end----------------------");
        System.out.println("--------------tx begin-------------------");
        if (txIDs != null) {
            index = 0;
            for (long txID : txIDs) {
                System.out.println("txIDs[" + index + "]: " + txID);
                index++;
            }
        }
        System.out.println("--------------tx end----------------------");

        System.out.println("top: " + topN);
        System.out.println("output: " + output);
        System.out.println("--------------colwidths begin-------------------");
        if (colWidths.size() != 0) {
            index = 0;
            Iterator<Entry<String, String>> entries = colWidths.entrySet().iterator();
            while (entries.hasNext()) {
                Entry<String, String> entry = entries.next();
                String key = entry.getKey();
                String value = entry.getValue();
                System.out.println(key + ":" + value);
            }
        }
        System.out.println("--------------colwidths end----------------------");
        System.out.println("debug: " + debug);
        System.out.println("mergeMode: " + mergeMode);
        System.out.println("waitTimeoutNum: " + waitTimeoutNum);
        System.out.println("displayimplictitsavepoint: " + displayimplictitsavepoint);
        System.out.println("displaysavepointid: " + displaysavepointid);
        System.out.println("--------------configparas begin-------------------");
        if (configparas.size() != 0) {
            index = 0;
            Iterator<Entry<String, String>> entries = configparas.entrySet().iterator();
            while (entries.hasNext()) {
                Entry<String, String> entry = entries.next();
                String key = entry.getKey();
                String value = entry.getValue();
                System.out.println(key + ":" + value);
            }
        }
        System.out.println("--------------configparas end----------------------");
        System.out.println("configfile: " + configfile);
    }
}
