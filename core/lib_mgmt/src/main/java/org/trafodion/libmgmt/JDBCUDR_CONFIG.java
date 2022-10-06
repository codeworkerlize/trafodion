/**********************************************************************
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
**********************************************************************/

package org.trafodion.libmgmt;

import java.io.File;
import java.io.FileOutputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
//import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.fs.permission.FsAction;

import org.trafodion.sql.udr.*;
import java.sql.*;
import java.util.Vector;
import java.lang.Math;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.io.PrintStream;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.UUID;
import java.nio.ByteBuffer;

import java.math.BigDecimal;

import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

//
// JDBC_PARALLEL connection configuration UDF
//
public class JDBCUDR_CONFIG extends UDR
{

    static class Parameter {

        String config_name_;
        String url_;
        String class_name_;
        String jar_name_;
        String user_name_;
        String user_pwd_;
        boolean is_list_;
        boolean is_delete_;

        public void setConfigName(String config_name) { config_name_ = config_name; }
        public void setUrl(String url)                { url_ = url; }
        public void setClassName(String class_name)   { class_name_ = class_name; }
        public void setJar(String jar_name)           { jar_name_ = jar_name; }
        public void setUserName(String user_name)     { user_name_ = user_name; }
        public void setUserPwd(String user_pwd)       { user_pwd_ = user_pwd; }
        public void setIsList(boolean is_list)        { is_list_ = is_list; }
        public void setIsDelete(boolean is_delete)    { is_delete_ = is_delete; }

        public String getConfigName() { return config_name_ ;}
        public String getUrl()        { return url_ ;}
        public String getClassName()  { return class_name_ ;}
        public String getJarName()    { return jar_name_ ;}
        public String getUserName()   { return user_name_ ;}
        public String getUserPwd()    { return user_pwd_ ;}
        public boolean getIsList()    { return is_list_ ;}
        public boolean getIsDelete()  { return is_delete_; }
    }

    // Define data that gets passed between compiler phases
    static class ConfigUDRCompileTimeData extends UDRWriterCompileTimeData
    {
        Parameter par_;

        ConfigUDRCompileTimeData()
        {
            par_ = new Parameter();
            par_.setIsList(false);
            par_.setIsDelete(false);
        }
    };

    Parameter getParameter(UDRInvocationInfo info) throws UDRException
    {
        return ((ConfigUDRCompileTimeData) info.getUDRWriterCompileTimeData()).par_;
    }

    @Override
    public void describeParamsAndColumns(UDRInvocationInfo info)
        throws UDRException
    {
        // This TMUDF takes no table-valued inputs, six string
        // parameters, and generates a table with a single varchar(200)
        // column. This TMUDF stores JDBC connection information in HDFS,
        // and list all connection configurations from HDFS.
        //   HDFS dir : "/user/trafodion/udr/jdbc/"
        //
        // It assumes that it was created with the following DDL,
        // without specifying parameters or return values:
        //
        //  create table_mapping function SyncLibUDF()
        //  external name 'JDBCUDR_CONFIG'
        //  library ...;
        //
        //
        // Example 01 : set JDBC connection information like below
        //
        //    select * from
        //    udf(jdbc_config('config_name',
        //                             'ojdbc6.jar',
        //                             'oracle.jdbc.driver.OracleDriver',
        //                             'jdbc:oracle:thin:@//IP:1522/orcl',
        //                             'username',
        //                             'password'));
        //
        //
        // Example 02 : list all JDBC connection config names
        //
        //    select * from
        //    udf(jdbc_config('list'));
        //
        //

        info.setUDRWriterCompileTimeData(new ConfigUDRCompileTimeData());

        handleInputParams(info, getParameter(info));

        for (int i=0; i<info.par().getNumColumns(); i++)
            info.addFormalParameter(info.par().getColumn(i));
        info.out().addVarCharColumn("RESULT", 200, false,
                                    TypeInfo.SQLCharsetCode.CHARSET_UTF8,
                                    TypeInfo.SQLCollationCode.SYSTEM_COLLATION);
    }


    @Override
    public void processData(UDRInvocationInfo info,
                            UDRPlanInfo plan)
        throws UDRException
    {
        boolean result = false;
        String HdfsConfDir = "/user/trafodion/udr/jdbc/";

        Parameter par = new Parameter();

        handleInputParams(info, par);

        if (par.getIsList())
        {
            result = listAllConfig(info, par, HdfsConfDir);
        }
        else if (par.getIsDelete())
        {
            result = deleteConfig(info, par, HdfsConfDir);
        }
        else
        {
            result = saveConfg(info, par, HdfsConfDir);
        }

    }

    boolean listAllConfig(UDRInvocationInfo info, Parameter par, String dir) throws UDRException {

        boolean result = false;

        org.apache.hadoop.fs.Path confDir = null;

        try {

            confDir    = new org.apache.hadoop.fs.Path(dir);

            Configuration conf = new Configuration(true);
            FileSystem  fs = FileSystem.get(conf);

            if (false == fs.exists(confDir))
                fs.mkdirs(confDir, new FsPermission(FsAction.ALL,
                                                    FsAction.NONE,
                                                    FsAction.NONE));

            FileStatus[] confFiles = fs.listStatus(confDir);

            for ( int i = 0; i < confFiles.length; i ++) {
                if (confFiles[i].isFile()) {
                    info.out().setString(0, confFiles[i].getPath().getName());
                    emitRow(info);
                }
            }

            result = true;
        }
        catch (Exception e) {
            throw new UDRException(38973, "Error list JDBC_PARALLEL config files from hdfs dir %s, reason: %s",
                                   (confDir != null) ? confDir.toString() : "null",
                                   e.getMessage());
        }

        return result;
    }

    boolean deleteConfig(UDRInvocationInfo info, Parameter par, String dir) throws UDRException {
    
        boolean result = false;

        org.apache.hadoop.fs.Path confDir = null;
        org.apache.hadoop.fs.Path confPath = null;

        try {
            String fileName = par.getConfigName();

            confDir    = new org.apache.hadoop.fs.Path(dir);
            confPath   = new org.apache.hadoop.fs.Path(dir, fileName);

            Configuration conf = new Configuration(true);
            FileSystem  fs = FileSystem.get(conf);

            if (false == fs.exists(confDir))
                fs.mkdirs(confDir, new FsPermission(FsAction.ALL,
                                                    FsAction.NONE,
                                                    FsAction.NONE));

            result = fs.delete(confPath, true);
        }
        catch (Exception e) {
            throw new UDRException(38973, "Error delete JDBC_PARALLEL config files from hdfs dir %s, reason: %s",
                                   (confDir != null) ? confDir.toString() : "null",
                                   e.getMessage());
        }

        if (result == true)
        {
            info.out().setString(0, "Success");
        }
        else
        {
            info.out().setString(0, "Fail");
        }
        emitRow(info);

        return result;
    }

    boolean saveConfg(UDRInvocationInfo info, Parameter par, String dir) throws UDRException {

        boolean result = false;

        org.apache.hadoop.fs.Path confDir = null;
        org.apache.hadoop.fs.Path confPath = null;

        try {
            String fileName = par.getConfigName();

            confDir    = new org.apache.hadoop.fs.Path(dir);
            confPath   = new org.apache.hadoop.fs.Path(dir, fileName);

            Configuration conf = new Configuration(true);
            FileSystem  fs = FileSystem.get(conf);

            if (false == fs.exists(confDir))
                fs.mkdirs(confDir, new FsPermission(FsAction.ALL,
                                                    FsAction.NONE,
                                                    FsAction.NONE));

            FSDataOutputStream dataFileStream = fs.create(confPath);

            StringBuilder confRecord = new StringBuilder();
            confRecord.append(par.getJarName() +"\n");
            confRecord.append(par.getClassName() +"\n");
            confRecord.append(par.getUrl() +"\n");
            confRecord.append(JDBCUDRUtil.encryptByAes(par.getUserName()) +"\n");
            confRecord.append(JDBCUDRUtil.encryptByAes(par.getUserPwd()) +"\n");

            dataFileStream.writeBytes(confRecord.toString());
            dataFileStream.flush();
            dataFileStream.close();

            result = true;
        }
        catch (Exception e) {
            throw new UDRException(38973, "Error save JDBC_PARALLEL config file to hdfs file %s, reason: %s",
                                   (confPath != null) ? confPath.toString() : "null",
                                   e.getMessage());
        }

        // return my own instance number as the result
        if (result == true)
        {
            info.out().setString(0, "Success");
        }
        else
        {
            info.out().setString(0, "Fail");
        }
        emitRow(info);

        return result;
    }

    void handleInputParams(UDRInvocationInfo info, Parameter par) throws UDRException {

        if (info.getNumTableInputs() > 0)
            throw new UDRException(38971, "This UDF needs to be called without table-valued inputs");

        if (info.par().getNumColumns() > 0 &&
            info.par().getString(0).compareTo("list") == 0)
        {
            par.setIsList(true);
        }
        else if (info.par().getNumColumns() > 0 &&
                 info.par().getString(0).compareTo("delete") == 0)
        {
            if (info.par().getNumColumns() != 2)
                throw new UDRException(38970, "delete operation need one JDBC config name");
            par.setIsDelete(true);
            par.setConfigName(info.par().getString(1));
        }
        else
        {
            if (info.par().getNumColumns() != 6)
                throw new UDRException(38970, "This UDF needs to be called with six input parameters, "+
                                              "like : \nudf(jdbc_config(\n"+
                                              "    'config_name',\n"+
                                              "    'ojdbc6.jar',\n"+
                                              "    'oracle.jdbc.driver.OracleDriver',\n"+
                                              "    'jdbc:oracle:thin:@//IP:1522/orcl',\n"+
                                              "    'username',\n"+
                                              "    'password'))");

            par.setConfigName(info.par().getString(0));
            par.setJar       (info.par().getString(1));
            par.setClassName (info.par().getString(2));
            par.setUrl       (info.par().getString(3));
            par.setUserName  (info.par().getString(4));
            par.setUserPwd   (info.par().getString(5));
        }
    }
}
