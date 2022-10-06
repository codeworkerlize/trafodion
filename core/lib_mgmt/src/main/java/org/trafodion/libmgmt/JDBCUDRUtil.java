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


public class JDBCUDRUtil {

    public static String encryptByAes(Object object) throws UDRException {
        try {
            StringBuffer sb =new StringBuffer();

            SecretKeySpec key = new SecretKeySpec("1234567890123456".getBytes(), "AES");
            Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
            byte[] byteContent = ((String) object).getBytes("utf-8");
            cipher.init(Cipher.ENCRYPT_MODE, key);

            byte[] result = cipher.doFinal(byteContent);
            for (int i = 0; i < result.length; i++) {
                String hex = Integer.toHexString(result[i] & 0xFF);
                if (hex.length() == 1) {
                    hex = '0' + hex;
                }
                sb.append(hex);
            }
            return sb.toString();
        } catch (Exception e) {
            //e.printStackTrace();
            throw new UDRException(
                    38000,
                    "encryptByAes failed, source %s, got error : %s",
                    ((String) object),
                    e.getMessage());
        }
    }

    public static String decryptByAes(String content) throws UDRException {
        try {
            SecretKeySpec key = new SecretKeySpec("1234567890123456".getBytes(), "AES");
            Cipher cipher = Cipher.getInstance("AES/ECB/PKCS5Padding");
            cipher.init(Cipher.DECRYPT_MODE, key);
            byte[] byteRresult = new byte[content.length() / 2];
            for (int i = 0; i < content.length() / 2; i++) {
                int high = Integer.parseInt(content.substring(i * 2, i * 2 + 1), 16);
                int low = Integer.parseInt(content.substring(i * 2 + 1, i * 2 + 2), 16);
                byteRresult[i] = (byte) (high * 16 + low);
            }
            byte[] result = cipher.doFinal(byteRresult);
            String originalString = new String(result,"UTF-8");
            return originalString;
        } catch (Exception e) {
            //e.printStackTrace();
            throw new UDRException(
                    38000,
                    "decryptByAes failed, source %s, got error : %s",
                    content,
                    e.getMessage());
        }
    }
}
