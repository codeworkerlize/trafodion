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

package org.trafodion.libmgmt;

import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.ArrayList;
import java.io.IOException;
import java.util.Collections;
import java.sql.Timestamp;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.math.BigDecimal;
import org.trafodion.sql.udr.spsql.BigNumBCDUtil;

import java.sql.*;

public class AlignedFormatTupleParser {

    private String colList;
    private String resultString;
    private ByteBuffer bb;
    private String delimiter = ",";
    private byte[] nullBitmap = null;
    private byte[] FFbytes = new byte[4];
    private byte[] rawdata_ = null ;
    private List<Integer> types = new ArrayList<Integer>();
    private List<Integer> ktypes = new ArrayList<Integer>();
    private List<Integer> nulls = new ArrayList<Integer>();
    private List<Integer> offsets = new ArrayList<Integer>();
    private List<String> charsets = new ArrayList<String>();
    private List<String> kcharsets = new ArrayList<String>();
    private List<Integer> ktablepos = new ArrayList<Integer>();
    private List<Integer> orderedktablepos = new ArrayList<Integer>();
    private List<Integer> precisions = new ArrayList<Integer>();
    private List<Integer> kprecisions = new ArrayList<Integer>();
    private List<Integer> scales = new ArrayList<Integer>();
    private List<Integer> kscales = new ArrayList<Integer>();
    private List<Integer> vcLens = new ArrayList<Integer>();
    private List<Integer> sizes = new ArrayList<Integer>();
    private List<Integer> ksizes = new ArrayList<Integer>();
    private List<Short> korders = new ArrayList<Short>();
    private List<Integer> knulls = new ArrayList<Integer>();
    private List<String> sqltype = new ArrayList<String>();
    private List<String> ksqltype = new ArrayList<String>();
    private List<Integer> dts= new ArrayList<Integer>();  //datetime_start_field
    private List<Integer> kdts= new ArrayList<Integer>();
    private List<Integer> des= new ArrayList<Integer>();  //datetime_end_field
    private List<Integer> kdes= new ArrayList<Integer>();
    private List<String> colNames = new ArrayList<String>();
    private List<String> kcolNames = new ArrayList<String>();

    private List<Integer> colIsSystem = new ArrayList<Integer>();
    private List<Integer> kcolIsSystem = new ArrayList<Integer>();
    private List<Integer> colIsAdded = new ArrayList<Integer>();
    private List<Integer> align8 = new ArrayList<Integer>();
    private List<Integer> align4 = new ArrayList<Integer>();
    private List<Integer> align2 = new ArrayList<Integer>();
    private List<Integer> align1 = new ArrayList<Integer>();
    private List<Integer> addedCols = new ArrayList<Integer>();
    private List<Integer> fixedColPos = new ArrayList<Integer>();

    private static final int FF_OFFSET = 0;
    private static final int FF_EXT_OFFSET = 2;
    private static final int FF_BO_OFFSET = 4;

    private int varColNum = 0;
    private int addVarColNum = 0;
    private int addFixColNum = 0;
    private int colNum = 0;
    private int kcolNum = 0;
    private int keyLength = 0;

    private static boolean doKeyReorder = true;

    private int versionSize = 7; //Qian1.6.3 has 7 attributes
    private int versionSizev2 = 12; //Qian1.6.4 has 12 attributes

    public static final int REC_MIN_NUMERIC = 128;
    public static final int REC_MIN_BINARY_NUMERIC = 130;
    public static final int REC_BIN16_SIGNED = 130;
    public static final int REC_BIN16_UNSIGNED = 131;
    public static final int REC_BIN32_SIGNED = 132;
    public static final int REC_BIN32_UNSIGNED = 133;
    public static final int REC_BIN64_SIGNED = 134;
    public static final int REC_BPINT_UNSIGNED = 135;	// Bit Precision Integer
    public static final int REC_BIN8_SIGNED = 136;     // tinyint signed
    public static final int REC_BIN8_UNSIGNED = 137;   // tinyint unsigned
    public static final int REC_BIN64_UNSIGNED = 138;
    public static final int REC_MAX_BINARY_NUMERIC =  138;
    public static final int REC_MIN_FLOAT = 142;
    public static final int REC_IEEE_FLOAT32 = 142;
    public static final int REC_IEEE_FLOAT64 = 143;
    public static final int REC_FLOAT32 = 142;
    public static final int REC_FLOAT64 = 143;
    public static final int REC_MAX_FLOAT =  143;
    public static final int REC_MIN_DECIMAL =  150;
    public static final int REC_DECIMAL_UNSIGNED = 150;
    public static final int REC_DECIMAL_LS = 151;
    public static final int REC_DECIMAL_LSE = 152;
    public static final int REC_NUM_BIG_UNSIGNED = 155;
    public static final int REC_NUM_BIG_SIGNED = 156;
    public static final int REC_MAX_DECIMAL = 156;
    public static final int REC_MAX_NUMERIC = 156;
    public static final int REC_MIN_CHARACTER = 0   ;   // MP REC_MIN_CHAR
    public static final int REC_MIN_F_CHAR_H =  0   ;   // MP same name
    public static final int REC_BYTE_F_ASCII = 0   ;   // MP same name
    public static final int REC_BYTE_F_DOUBLE =  2   ;   // MP same name
    public static final int REC_NCHAR_F_UNICODE =    2	;// [MXsynonym]
    public static final int REC_BINARY_STRING =   9;
    public static final int REC_MAX_F_CHAR_H  =  47;      // MP same name
    public static final int REC_MIN_V_CHAR_H  =  64;      // MP same name
    public static final int REC_BYTE_V_ASCII  =  64;      // MP same name
    public static final int REC_BYTE_V_DOUBLE =  66;      // MP same name
    public static final int REC_NCHAR_V_UNICODE =  66;	// [MXsynonym]
    public static final int REC_VARBINARY_STRING = 69;
    public static final int REC_BYTE_V_ASCII_LONG = 70;      // MX only: ODBC LONG VARCHAR
    public static final int REC_MIN_V_N_CHAR_H = 100;	// MX only: nul-term. subrange
    public static final int REC_BYTE_V_ANSI = 100;      // MX only: nul-terminated
    public static final int REC_BYTE_V_ANSI_DOUBLE =  101;      // MX only: nul-terminated
    public static final int REC_NCHAR_V_ANSI_UNICODE =   101;	// [MXsynonym]
    public static final int REC_MAX_V_N_CHAR_H     =     111;	// MX only: nul-term. subrange
    public static final int REC_MAX_V_CHAR_H       =     111;      // MP same name
    public static final int REC_MAX_CHARACTER      =     115;      // MX value: see LOCALEs below!
    public static final int REC_MAX_CHARACTER_MP   =     127;      // MP REC_MAX_CHAR
    public static final int REC_SBYTE_LOCALE_F	    =  124;
    public static final int REC_MBYTE_LOCALE_F	    =  125;
    public static final int REC_MBYTE_F_SJIS	    =  126;
    public static final int REC_MBYTE_V_SJIS	    =  127;
    public static final int REC_BLOB               =     160;      // SQ only: blob datatype
    public static final int REC_CLOB               =     161;      // SQ only: clob datatype
    public static final int REC_BOOLEAN            =     170;
    public static final int REC_DATETIME           =     192;
    public static final int REC_MIN_INTERVAL       =195;
    public static final int REC_INT_YEAR           =195;
    public static final int REC_INT_MONTH          =196;
    public static final int REC_INT_YEAR_MONTH     =197;
    public static final int REC_INT_DAY            =198;
    public static final int REC_INT_HOUR           =199;
    public static final int REC_INT_DAY_HOUR       =200;
    public static final int REC_INT_MINUTE         =201;
    public static final int REC_INT_HOUR_MINUTE    =202;
    public static final int REC_INT_DAY_MINUTE     =203;
    public static final int REC_INT_SECOND         =204;
    public static final int REC_INT_MINUTE_SECOND  =205;
    public static final int REC_INT_HOUR_SECOND    =206;
    public static final int REC_INT_DAY_SECOND     =207;
    public static final int REC_INT_FRACTION       =208;     // Used in MP only! 
    public static final int REC_MAX_INTERVAL       =208 ;    
    public static final int REC_MAX_INTERVAL_MP    =212 ;  
    public static final int REC_ARRAY              =220;
    public static final int REC_ROW                =221;
    public static final int REC_UNKNOWN            =-1;

    private long ts_;

    private boolean initialized = false;
    private boolean kinitialized = false;
    private boolean checkNull = false;

    private boolean noPk = false;

    private boolean valid_ = true;

    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

    public static String bytesToHex(byte[] bytes) {
      char[] hexChars = new char[bytes.length * 2];
      for (int j = 0; j < bytes.length; j++) {
        int v = bytes[j] & 0xFF;
        hexChars[j * 2] = HEX_ARRAY[v >>> 4];
        hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
      }
      return new String(hexChars);
    }

    private byte[] getBytesFromRaw(int pos, int len) {
      byte[] ret = new byte[len];
      System.arraycopy(rawdata_, pos, ret, 0, len);
      return ret;
    }

    //constructor
    public AlignedFormatTupleParser()  
    {
    }

    public void setValid(boolean b) { valid_ = b; }
    public boolean getValid() { return valid_; }

    public void setNoPk(boolean v) { noPk = v; }
    public boolean getNoPk() { return noPk ; }

    public void setDelimeter(String d) { delimiter = d; }

    //used to parse the primary key
    public byte[] flipAllBit(byte[] data)
    {
       for(int i=0; i < data.length; i++)
       {
         data[i] = (byte) ~data[i];
       }
       return data;
    }
    public byte[] flipFirstBit(byte[] data)
    {
      byte ccc = (byte)0x80;
      data[0] = (byte)(data[0] ^ ccc);
      return data;
    }

    private boolean checkFirstBit(byte[] data)
    {
      if( ( data[0] & 0x80) == 0x80) return true;
      return false;
    }

    public int getFF()
    {
      byte ccc = 0x3;
      FFbytes[1] = (byte)(FFbytes[1] & ccc);
      ByteBuffer ffb = ByteBuffer.wrap(FFbytes).order(ByteOrder.LITTLE_ENDIAN); 
      int v1 = ffb.getInt(0);
      return v1;
    }

    public int getFirstFixOffset()
    {
      int ff = getFF();
      return ff;
    }

    public int getBitmapOffset()
    {
      return bb.getInt(FF_BO_OFFSET);
    }

    private int getAlignedSize(int offset, int pos, int size) {
        int realOffset = offset + size;
        int nextColPos = fixedColPos.get(pos + 1);
        int isNextAdded = colIsAdded.get(nextColPos);
        if(isNextAdded == 1) {
            int nextSize = sizes.get(nextColPos);
            if (nextSize % 4 == 0) {
                int remainder = realOffset % 4;
                if (remainder != 0) {
                    realOffset += 4 - remainder;
                }
            }
        }
        return realOffset;
    }

    public int getVO(int pos)
    {
      int vpos = 0, i = 0;
      for(int t : types) {
        if(t >=REC_MIN_V_CHAR_H && t< REC_MAX_V_CHAR_H )
        {
          if(i<pos) 
            vpos++;
          else
            break;
        } 
        i++;
      } 
      return 8 + vpos*4;
    }

    public int getColNum()
    {
      return colNum;
    }
	
    public void setSchema(String colList)
    {
        parseColList(colList);
        orderFixedFieldsByAlignment();
    }

    public void setKey(String keyList)
    {
        parseKeyList(keyList);
    }

    public void populateNullBitmap(byte[] src)
    {
        System.arraycopy(src, 0 , FFbytes , 0, 4);
        rawdata_ = src;
    }

    public boolean checkIfIsNull(int colPos)
    {
      //check the real pos of this col
      if(nulls.get(colPos) == 0) //not null
        return false;

      int nPos = 0;
      for(int i =0; i < nulls.size(); i++) {
        if(nulls.get(i) == 1) //nullable
        {
          if(i < colPos) nPos++;
          else break;
        } 
      }
      int byteIndex = nPos/8;
      int bitIndex = (8 - (nPos % 8) ) -1;
      byte c = (byte)(1 << bitIndex);
      
      int bo =  getBitmapOffset();
      byte b = rawdata_[byteIndex + bo];
      return ((b & c) == c);
    }

    public String getStringFromBB(int pos, int len)
    {
      return new String(rawdata_, pos, len);
    }

    public String getStringFromBB(int pos, int len, byte[] rk, short flip)
    {
      if(flip == 1)
      {
         //flip the bytes starting from pos
         byte[] src =  new byte[len];
         System.arraycopy(rk, pos, src, 0, len);
         src = flipAllBit(src);
         return new String(src);
      }
      else
        return new String(rk, pos, len);
    }

    public String getUCS2StringFromBB(int pos, int len)
    {
      try {
        return new String(rawdata_, pos, len, "UTF-16LE");
      }
      catch(Exception e) { return ""; }
    }
    public String getUCS2StringFromBB(int pos, int len, byte[] rk ,short flip)
    {
      try {
        if(flip == 1)
        {
          //flip the bytes starting from pos
          byte[] src =  new byte[len];
          System.arraycopy(rk, pos, src, 0, len);
          src = flipAllBit(src);
          return new String(src, 0, len,  "UTF-16BE");
        }
        else
          return new String(rk, pos, len, "UTF-16BE");
      }
      catch(Exception e) { return ""; }
    }

    public void setRawData(byte[] rawData)
    {
        bb = ByteBuffer.wrap(rawData).order(ByteOrder.LITTLE_ENDIAN);
        rawdata_ = new byte[rawData.length];
        System.arraycopy(rawData, 0, rawdata_ , 0, rawData.length);
        populateNullBitmap(rawData);
    }

    public String getKey(byte[] rk)
    {
      boolean keynullable = false;

      if(keyLength < rk.length) {
        keynullable = true;
      }
        bb = ByteBuffer.wrap(rk);
        String part = "";
        resultString = "";
        String originParts[] = new String[kcolNum];
        byte ccc = (byte)0x80;

        for(int i=0; i< kcolNum ; i++)
        {
           if(kcolIsSystem.get(i) == 1) { originParts[i]=""; continue; }
           int type = ktypes.get(i);
           int colPos = getKColOffset(i, keynullable);
           short keyOrder = korders.get(i);
           if(keynullable == true && knulls.get(i) == 1)
           {
             if(keyOrder == 0) //normal
             {
               if(rk[colPos] == -1 ) {  //null
                 originParts[i] = "NULL";
                 continue;
               }
             } //nullable key use different null indicator
             else 
             {
               if(rk[colPos] == 0 ) {  //null
                 originParts[i] = "NULL";
                 continue;
               }
             }
             colPos += 2;  //bypass the null indicator
           }
           int len = ksizes.get(i);
           int scale = kscales.get(i);
           int p = kprecisions.get(i);
           byte[] flipbytes ;
           ByteBuffer flipbb;
           String addSign="";

           String mycharset = kcharsets.get(i);
           switch(type) {
               case REC_BIN8_SIGNED:
                 byte sccc = bb.get(colPos);
                 if(keyOrder == 1)
                   sccc =  (byte) ~sccc;
                 sccc = (byte)(sccc ^ ccc);
                 if( scale == 0)
                   part = String.valueOf(sccc);
                 else {
                   double dc = sccc /  Math.pow(10,scale);
                   part = Double.toString(dc);
                 }
                 break;
               case REC_BIN8_UNSIGNED:
                 byte uccc = (byte)(bb.get(colPos) & 0xff);
                 if(keyOrder == 1)
                   uccc =  (byte) ~uccc;
                 uccc = (byte)(uccc ^ ccc);
                 if( scale == 0)
                   part = String.valueOf(uccc);
                 else {
                   double udc = uccc /  Math.pow(10,scale);
                   part = Double.toString(udc);
                 }
                 break;
               case REC_FLOAT32:
                 flipbytes = new byte[4];
                 System.arraycopy(rk, colPos, flipbytes, 0, 4);
                 if(keyOrder == 1) 
                   flipbytes = flipAllBit(flipbytes);
                 flipbytes = flipFirstBit(flipbytes);
                 if(checkFirstBit(flipbytes) == true) //negative value flip all bits
                 {
                   flipbytes = flipAllBit(flipbytes);
                   addSign="-";
                 }
                 flipbb = ByteBuffer.wrap(flipbytes);
                 float floatv = flipbb.getFloat(0);
                 part = addSign+Float.toString(floatv);
                 break;
               case REC_FLOAT64:
                 flipbytes = new byte[8];
                 System.arraycopy(rk, colPos, flipbytes, 0, 8);
                 if(keyOrder == 1) 
                   flipbytes = flipAllBit(flipbytes);
                 flipbytes = flipFirstBit(flipbytes);
                 if(checkFirstBit(flipbytes) == true) //negative value flip all bits
                 {
                   flipbytes = flipAllBit(flipbytes);
                   addSign="-";
                 }
                 flipbb = ByteBuffer.wrap(flipbytes);
                 double dv = flipbb.getDouble(0);
                 part = addSign+Double.toString(dv);
                 break;
               case REC_BIN16_SIGNED:
                 byte[] shortd = new byte[2];
                 System.arraycopy(rk, colPos , shortd, 0, 2);
                 if(keyOrder == 1)
                   shortd = flipAllBit(shortd);
                 shortd = flipFirstBit(shortd);
                 ByteBuffer shortbb = ByteBuffer.wrap(shortd);
                 short shortv = shortbb.getShort(0);
                 if( scale == 0) 
                   part = Short.toString(shortv);
                 else {
                   double d16v = shortv / Math.pow(10,scale);
                   part = Double.toString(d16v);
                 }
                 break;
               case REC_BIN16_UNSIGNED:
                 if(keyOrder == 0) {
                   int usv = bb.getInt(colPos) & 0xffff;
                   if( scale == 0) 
                     part = Integer.toString(usv);
                   else {
                     double d16uv = usv / Math.pow(10,scale);
                     part = Double.toString(d16uv);
                   }
                 }
                 else
                 {
                   flipbytes = new byte[2];
                   System.arraycopy(rk, colPos, flipbytes, 0, 2);
                   flipbytes = flipAllBit(flipbytes);
                   flipbb = ByteBuffer.wrap(flipbytes);
                   int usv = flipbb.getInt(0) & 0xffff;
                   if( scale == 0) 
                     part = Integer.toString(usv);
                   else {
                     double d16uv = usv / Math.pow(10,scale);
                     part = Double.toString(d16uv);
                   }
                 }
                 break;
               case REC_BIN32_SIGNED:
                 byte[] intd = new byte[4];
                 System.arraycopy(rk, colPos , intd, 0, 4);
                 if(keyOrder == 1)
                   intd = flipAllBit(intd);
                 intd = flipFirstBit(intd);
                 ByteBuffer intbb = ByteBuffer.wrap(intd);
                 int iv = intbb.getInt(0);
                 if( scale == 0) 
                   part = Integer.toString(iv);
                 else {
                   double d32v = iv / Math.pow(10,scale);
                   part = Double.toString(d32v);
                 }
                 break;
               case REC_BIN32_UNSIGNED:
                 if(keyOrder == 0) {
                   ByteBuffer uintbb = ByteBuffer.wrap(rk);
                   long uiv = uintbb.getInt(colPos) & 0xffffffffL;
                   if( scale == 0) 
                     part = Long.toString(uiv);
                   else {
                     double d32uv = uiv / Math.pow(10, scale);
                     part = Double.toString(d32uv);
                   }
                 }
                 else {
                   flipbytes = new byte[4];
                   System.arraycopy(rk, colPos, flipbytes, 0, 4);
                   flipbytes = flipAllBit(flipbytes);
                   flipbb = ByteBuffer.wrap(flipbytes);
                   long uiv = flipbb.getInt(0) & 0xffffffffL;
                   if( scale == 0)
                     part = Long.toString(uiv);
                   else {
                     double d32uv = uiv / Math.pow(10, scale);
                     part = Double.toString(d32uv);
                   }
                 }
                 break;
               case REC_BIN64_SIGNED:
                 byte[] longd = new byte[8];
                 System.arraycopy(rk, colPos , longd, 0, 8);
                 if(keyOrder == 1)
                   longd = flipAllBit(longd);
                 longd = flipFirstBit(longd);
                 ByteBuffer longbb = ByteBuffer.wrap(longd);
                 if(scale == 0) {
                   long lv = longbb.getLong(0);
                   part = Long.toString(lv) ;
                 }
                 else
                 {
                   long lv = longbb.getLong(0);
                   double b64uv = lv / Math.pow(10, scale);
                   part = Double.toString(b64uv);
                 }
                 break;
               case REC_BIN64_UNSIGNED:
                 if(keyOrder == 0) {
                   ByteBuffer ulongbb = ByteBuffer.wrap(rk);
                   long ulv64 = ulongbb.getLong(colPos);
                   BigDecimal bd = new BigDecimal(ulv64);
                   if(scale == 0)
                     part = bd.toString();
                   else {
                     Double lsca = Math.pow(10, scale);
                     BigDecimal sca = new BigDecimal(lsca);
                     BigDecimal theResult = bd.divide(sca);
                     double indoublev = theResult.doubleValue();
                     part = Double.toString(indoublev);
                   }
                 }
                 else {
                   flipbytes = new byte[8];
                   System.arraycopy(rk, colPos, flipbytes, 0, 8);
                   flipbytes = flipAllBit(flipbytes);
                   flipbb = ByteBuffer.wrap(flipbytes);
                   long ulv64 = flipbb.getLong(0);
                   BigDecimal bd = new BigDecimal(ulv64);
                   if(scale == 0)
                     part = bd.toString();
                   else {
                     Double lsca = Math.pow(10, scale);
                     BigDecimal sca = new BigDecimal(lsca);
                     BigDecimal theResult = bd.divide(sca);
                     double indoublev = theResult.doubleValue();
                     part = Double.toString(indoublev);
                   }
                 }
                 break;
               case REC_DATETIME:
                 if(len == 4) //DATE
                 {
                   part = "'" +  getDateInKey(colPos,rk, keyOrder) + "'";
                 }
                 else if(len == 7 || len == 11)  //TIMESTAMP[0-6]
                 {
                   part = "'" + getTimestampInKey(colPos, scale, rk, keyOrder) + "'";
                 }
                 else
                   part = "NotSupportYet";
                 break;
               case REC_DECIMAL_UNSIGNED:
                 String udecstr = getStringFromBB(colPos, len, rk, keyOrder);

                 long udlv = Long.parseLong(udecstr);
                 if( scale == 0)
                 {
                   part = Long.toString(udlv);
                 }
                 else {
                   StringBuffer sb = new StringBuffer(udecstr);
                   sb.insert(p-scale,'.');
                   int zi = 0;
                   while(zi<udecstr.length() && udecstr.charAt(zi)=='0')
                     zi++;
                   if(zi == p - scale ) zi--;
                   sb.replace(0, zi, "");
                   part = "" + sb;
                 }
                break;
               case REC_DECIMAL_LSE:
                 byte[] decimalbytes = new byte[len];
                 System.arraycopy(rk, colPos, decimalbytes , 0, len);
                 if(keyOrder == 1) //DESC
                   decimalbytes = flipAllBit(decimalbytes);
                 byte c = decimalbytes[0];
                 String sign2 = "";
                 if( (c & 0x80 ) != 0x80) 
                   sign2 = "-";
                 if( sign2.equals("-") )
                 {
                   decimalbytes = flipAllBit(decimalbytes);
                   decimalbytes = flipFirstBit(decimalbytes);
                 }
                 else
                   decimalbytes = flipFirstBit(decimalbytes);
                 String decstr = new String(decimalbytes, 0 , len);
                 long dlv = Long.parseLong(decstr);
                 if(scale == 0)
                 {
                   part = sign2 + Long.toString(dlv);
                 }
                 else {
                   StringBuffer sb = new StringBuffer(decstr);
                   sb.insert(p-scale,'.');
                   int zii = 0;
                   while(zii < sb.length() && sb.charAt(zii)=='0')
                     zii++;
                   if(zii == p - scale ) zii--;
                   if(zii >0)
                     sb.replace(0, zii, "");
                   else
                   {
                     if(sb.charAt(0) == '.') //append a 0
                       sb.insert(0,'0');
                   }
                   part = sign2 + sb;
                 }
                 break;
               case REC_BYTE_F_ASCII:
               case REC_NCHAR_F_UNICODE:
                 if(mycharset.equals("UCS2"))
                 {
                   String bsf1 = getUCS2StringFromBB(colPos, len, rk, keyOrder);
                   part = "'" + bsf1 + "'";
                 }
                 else {
                   String  sf = getStringFromBB(colPos, len, rk, keyOrder);
                   //truncate to p
                   if(p>0 && p < len)
                     sf = sf.substring(0,p);
                   part = "'" + sf + "'";
                 }
                 break;
               case REC_VARBINARY_STRING:
               case REC_BYTE_V_ASCII:
               case REC_NCHAR_V_UNICODE:
                 if(mycharset.equals("UCS2"))
                 {
                   String bsf1 = getUCS2StringFromBB(colPos, len, rk, keyOrder); 
                   //for varchar type, it needs to trim the spaces
                   //this is not a correct behavior, but should be correct in 99% use cases
                   //in the long run, we should get this value from getValue, but it is too complex
                   bsf1 = rtrim(bsf1);
                   part = "'" + bsf1 + "'";
                 }
                 else {
                   String  sf = getStringFromBB(colPos, len, rk, keyOrder);
                   //truncate to p
                   if(p>0 && p < len)
                     sf = sf.substring(0,p);
                   //for varchar type, it needs to trim the spaces
                   sf = rtrim(sf);
                   part = "'" + sf + "'";
                 }
                 break;
               default:
                 part = "NotSupportYet";
                 break;
             }
           originParts[i] = part;
           part = "";
        }
        return reorderKeyOutput(originParts);
    }

    private String reorderKeyOutput(String[] res) {
      String rets="";
      if(doKeyReorder == true) { //reorder the output , by column's col_number in table definition
        String orderArray[] = new String[kcolNum];
        Integer kreorderIsSystem[] = new Integer[kcolNum];

        for(int i=0; i< kcolNum ; i++) {
          orderArray[getKeyPosInTable(ktablepos.get(i))] = res[i];
          if(kcolIsSystem.get(i) == 1) kreorderIsSystem[getKeyPosInTable(ktablepos.get(i))] = 1;
          else kreorderIsSystem[getKeyPosInTable(ktablepos.get(i))] = 0;
        }      
      
        for(int i=0; i< kcolNum ; i++) {
          if(kreorderIsSystem[i] == 1 ) continue;  //not output sys col
          rets = rets + orderArray[i];
          if(i < kcolNum-1)
          {
            if(kreorderIsSystem[i+1] == 0) // if last is not system col 
              rets += delimiter;
          }
        }
      }
      else{
        for(int i=0; i< kcolNum ; i++) {
          if(kcolIsSystem.get(i) == 1) continue;
          rets = rets + res[i];
          if(i < kcolNum-1 && kcolIsSystem.get(kcolNum-1) == 0)
           rets += delimiter;
        }
      }
      return rets;
    }

    private int getKeyPosInTable(int idx) {
      int ret = 0;
      for(int i=0; i< kcolNum; i++) {
        if(orderedktablepos.get(i) == idx )
        {
          ret = i;
          break;
        }
      }
      return ret;
    }

    private String getDateInKey(int pos, byte[] rk, short flip) {
      if(flip == 0) {
        ByteBuffer timebuffer = ByteBuffer.wrap(rk);
        int y = timebuffer.getShort(pos);
        byte m = rk[pos+2];
        byte d = rk[pos+3];
        return String.format("%d-%02d-%02d", y,m,d);
      }
      else {
        //first copy the bytes
        byte[] flipbytes = new byte[4];
        System.arraycopy(rk, pos, flipbytes, 0 , 4);
        flipbytes = flipAllBit(flipbytes);
        ByteBuffer timebuffer = ByteBuffer.wrap(flipbytes);
        int y = timebuffer.getShort(0);
        byte m = flipbytes[2];
        byte d = flipbytes[3];
        return String.format("%d-%02d-%02d", y,m,d);
      }
    }

    private String getDate(int pos) {
      int y = rawdata_[pos] & 0xFF | (rawdata_[pos+1]  & 0xFF ) << 8;
      byte m = rawdata_[pos+2];
      byte d = rawdata_[pos+3];
      return String.format("%d-%02d-%02d", y,m,d);
    }

    private String getTimestampInKey(int pos , int scale, byte[] rk, short flip) {
      if(flip == 0) {
        //11 bytes
        ByteBuffer timebuffer = ByteBuffer.wrap(rk);
        int y = timebuffer.getShort(pos);
        byte m = rk[pos+2];
        byte d = rk[pos+3];
        byte h = rk[pos+4];
        byte mm = rk[pos+5];
        byte s = rk[pos+6];
        int ms = 0;
        if(scale > 0) 
          ms = timebuffer.getInt(pos+7);
        String pattern = "%d-%02d-%02d %02d:%02d:%02d";
        if (scale > 1 && scale <= 6){
          pattern = pattern + ".%0" + scale + "d";
          return String.format(pattern, y,m,d,h,mm,s,ms);
        }else if (scale != 0){
          pattern = pattern + ".%d";
          return String.format(pattern, y,m,d,h,mm,s,ms);
        }
        return String.format(pattern, y,m,d,h,mm,s);
      }
      else
      {
        byte[] flipbytes = new byte[11];
        if(scale >0) 
          System.arraycopy(rk, pos, flipbytes, 0, 11);
        else
          System.arraycopy(rk, pos, flipbytes, 0, 7);
        flipbytes = flipAllBit(flipbytes);
        //11 bytes
        ByteBuffer timebuffer = ByteBuffer.wrap(flipbytes);
        int y = timebuffer.getShort(0);
        byte m = flipbytes[2];
        byte d = flipbytes[3];
        byte h = flipbytes[4];
        byte mm = flipbytes[5];
        byte s = flipbytes[6];
        int ms = 0;
        if(scale > 0) 
          ms = timebuffer.getInt(7);
        String pattern = "%d-%02d-%02d %02d:%02d:%02d";
        if (scale > 1 && scale <= 6){
          pattern = pattern + ".%0" + scale + "d";
          return String.format(pattern, y,m,d,h,mm,s,ms);
        }else if (scale != 0){
          pattern = pattern + ".%d";
          return String.format(pattern, y,m,d,h,mm,s,ms);
        }
        return String.format(pattern, y,m,d,h,mm,s);
      }
    }

    private String getTimestamp(int pos , int scale) {
      //11 bytes
      int y = rawdata_[pos] & 0xFF | (rawdata_[pos+1]  & 0xFF ) << 8;
      byte m = rawdata_[pos+2];
      byte d = rawdata_[pos+3];
      byte h = rawdata_[pos+4];
      byte mm = rawdata_[pos+5];
      byte s = rawdata_[pos+6];
      int ms = 0;
      if(scale >0)
        ms = bb.getInt(pos+7);
      String pattern = "%d-%02d-%02d %02d:%02d:%02d";
      if (scale > 1 && scale <= 6){
          pattern = pattern + ".%0" + scale + "d";
          return String.format(pattern, y,m,d,h,mm,s,ms);
      }else if (scale != 0){
          pattern = pattern + ".%d";
          return String.format(pattern, y,m,d,h,mm,s,ms);
      }
      return String.format(pattern, y,m,d,h,mm,s);
    }

    //given aligned format rawdata as byte array, return Strings of values that can be used in VALUES() clause
    public String getValue(byte[] rawData) 
    {
        bb = ByteBuffer.wrap(rawData).order(ByteOrder.LITTLE_ENDIAN);
        String part = "";
        resultString = "";
        int bo = getBitmapOffset();

        populateNullBitmap(rawData);

        for(int i=0; i< colNum ; i++)
        {
           if(colIsSystem.get(i) == 1) continue;
           boolean isNull = false;
           if(bo > 0) 
           {
              isNull = checkIfIsNull(i); 
           }
           if(! isNull) {
             int type = types.get(i);
             int colPos = getColOffset(i);
             int scale = scales.get(i);
             int p = precisions.get(i);
             int len = sizes.get(i);
             String mycharset = charsets.get(i);
             switch(type) {
               case REC_BIN8_SIGNED:
                 byte sccc = bb.get(colPos);
                 if( scale == 0) 
                   part = String.valueOf(sccc);
                 else {
                   double dc = sccc /  Math.pow(10,scale); 
                   part = Double.toString(dc);
                 }
                 break;
               case REC_BIN8_UNSIGNED:
                 byte uccc = (byte)(bb.get(colPos) & 0xff);
                 if( scale == 0) 
                   part = String.valueOf(uccc);
                 else {
                   double udc = uccc /  Math.pow(10,scale); 
                   part = Double.toString(udc);
                 }
                 break;
               case REC_FLOAT32:
                 float fv = bb.getFloat(colPos);
                 part = Float.toString(fv);
                 break;
               case REC_FLOAT64:
                 double dv = bb.getDouble(colPos);
                 part = Double.toString(dv);
                 break;
               case REC_BIN16_SIGNED:
                 short shortv = bb.getShort(colPos);
                 if( scale == 0) 
                   part = Short.toString(shortv);
                 else {
                   double d16v = shortv / Math.pow(10,scale);
                   part = Double.toString(d16v);
                 }
                 break;
               case REC_BIN16_UNSIGNED:
                 int usv = bb.getInt(colPos) & 0xffff;
                 if( scale == 0) 
                   part = Integer.toString(usv);
                 else {
                   double d16uv = usv / Math.pow(10,scale);
                   part = Double.toString(d16uv);
                 }
                 break;
               case REC_BIN32_SIGNED:
                 int iv = bb.getInt(colPos);
                 if( scale == 0) 
                   part = Integer.toString(iv);
                 else {
                   double d32v = iv / Math.pow(10,scale);
                   part = Double.toString(d32v);
                 }
                 break;
               case REC_BIN32_UNSIGNED:
                 long ulv = bb.getInt(colPos) & 0xffffffffL;
                 if( scale == 0) 
                   part = Long.toString(ulv);
                 else {
                   double d32uv = ulv / Math.pow(10, scale);
                   part = Double.toString(d32uv);
                 }
                 break;
               case REC_BIN64_UNSIGNED:
                 long ulv64 = bb.getLong(colPos);
                 BigDecimal bd = new BigDecimal(ulv64);
                 if(scale == 0)
                   part = bd.toString();
                 else {
                   Double lsca = Math.pow(10, scale);
                   BigDecimal sca = new BigDecimal(lsca);
                   BigDecimal theResult = bd.divide(sca);
                   double indoublev = theResult.doubleValue();
                   part = Double.toString(indoublev);
                 }
                 break;
               case REC_BIN64_SIGNED:
                 if(scale == 0) {
                   long lv = bb.getLong(colPos);
                   part = Long.toString(lv) ;
                 }
                 else
                 {
                   long lv = bb.getLong(colPos);
                   double b64uv = lv / Math.pow(10, scale);
                   part = Double.toString(b64uv);
                 }
                 break;
               case REC_DECIMAL_UNSIGNED:
                 String udecstr = getStringFromBB(colPos, len);
                 long udlv = Long.parseLong(udecstr);
                 if( scale == 0) 
                 {
                   part = Long.toString(udlv);
                 }
                 else {
                     StringBuffer sb = new StringBuffer(udecstr);
                     String res = null;
                     if (p == scale) {
                         sb.insert(p - scale, "0.");
                         res = sb.toString();
                     } else {
                         sb.insert(p - scale, '.');
                         res = sb.toString().replaceAll("^(0+)", "");
                         if(res.length()>0 && res.charAt(0) == '.') res = '0'+res; 
                     }
                     part = "" + res;
                 }
                break;
               case REC_DECIMAL_LSE:
                 byte[] decimalbytes = new byte[len];
                 System.arraycopy(rawdata_, colPos, decimalbytes , 0, len);
                 byte c = decimalbytes[0];
                 String sign2 = "";
                 if( (c & 0x80 ) == 0x80) sign2 = "-";
                 if( sign2.equals("-") )
                   decimalbytes = flipFirstBit(decimalbytes);
                 String decstr = new String(decimalbytes, 0 , len);
                 long dlv = Long.parseLong(decstr);
                 if(scale == 0) 
                 {
                   part = sign2 + Long.toString(dlv);
                 }
                 else {
                     StringBuffer sb = new StringBuffer(decstr);
                     String res = null;
                     if (p == scale) {
                         sb.insert(p - scale, "0.");
                         res = sb.toString();
                     } else {
                         sb.insert(p - scale, '.');
                         res = sb.toString().replaceAll("^(0+)", "");
                         if(res.length()>0 && res.charAt(0) == '.') res = '0'+res; 
                     }
                     part = sign2 + res;
                 }
                 break;
               case REC_BYTE_V_ASCII:
               case REC_VARBINARY_STRING:
                 int vclen = 4; //vcLens.get(i);
                 if(vclen == 2)
                 {
                   len = bb.getShort(colPos);
                   colPos += 2;
                 }
                 else
                 {
                   len = bb.getInt(colPos);
                   colPos += 4;
                 } 
                 if(mycharset.equals("UTF8")){
                   String sv = getStringFromBB(colPos, len);
                   //truncate to p
                   if(p>0 && p < len)
                     sv = sv.substring(0,p); 
                   part = "'" + sv + "'";
                 }
                 else {
                   String sv = getStringFromBB(colPos, len);
                   part = "'" + sv + "'";
                 }
                 break;
               case REC_BYTE_F_ASCII:
                 if(mycharset.equals("UTF8")){
                   String sf = getStringFromBB(colPos, len);
                   //truncate to p
                   if(p>0)
                     sf = sf.substring(0,p); 
                   part = "'" + sf + "'";
                 }
                 else {
                   String sf = getStringFromBB(colPos, len);
                   part = "'" + sf + "'";
                 }
                 break;
               case REC_DATETIME:
                 if(len == 4) //DATE
                 {
                   part = "'" +  getDate(colPos) + "'";
                 }
                 else if(len == 7 || len == 11)  //TIMESTAMP[0-6]
                 {
                   part = "'" + getTimestamp(colPos, scale) + "'";
                 }
                 else
                   part = "NotSupportYet";
                 break;
              case REC_NCHAR_F_UNICODE:
                 if(mycharset.equals("UCS2"))
                 {
                   String sf1 = getUCS2StringFromBB(colPos, len);
                   part = "'" + sf1 + "'";
                 }
                 else
                 { 
                   String sf1 = getStringFromBB(colPos, len);
                   part = "'" + sf1 + "'";
                 }
                 break;
              case REC_NCHAR_V_UNICODE:
                 int ncharvclen = 4; //vcLens.get(i);
                 if(ncharvclen == 2)
                 {
                   len = bb.getShort(colPos);
                   colPos += 2;
                 }
                 else
                 {
                   len = bb.getInt(colPos);
                   colPos += 4;
                 } 
                 if(mycharset.equals("UCS2"))
                 {
                   String ssf = getUCS2StringFromBB(colPos, len);
                   part = "'" + ssf + "'";
                 }
                 else
                 {
                   String ncharsv = getStringFromBB(colPos, len);
                   part = "'" + ncharsv + "'";
                 }
                 break;
               case REC_NUM_BIG_SIGNED:
               case REC_NUM_BIG_UNSIGNED:
                 byte[] bignumberarray = new byte[len];
                 System.arraycopy(rawdata_, colPos, bignumberarray , 0, len);
                 try {
                   part =  BigNumBCDUtil.convBigNumToAsciiMxcs(bignumberarray , len, p, scale);
                 } catch (Exception e) {part = "Exception";}
                 if(p == scale) 
                 {
                   if(part.charAt(0) == '-') {
                     StringBuilder strBuilder = new StringBuilder(part);
                     strBuilder.setCharAt(0, '0');
                     part = "-" + strBuilder.toString();
                   }
                   else
                     part = "0" + part;
                 }
                 break;
               default:
                 part = "NotSupportYet";
                 break; 
             }
           }
           else
             part = "NULL";

           if(i < colNum -1)
           {
             if(colIsSystem.get(i+1) != 1)
             part = part + delimiter;
           }
           resultString = resultString + part;

           part = "";
        }

        return resultString;
    }

    private int getColSize(int pos) {
      if(pos<0) return 0;
      int ret = sizes.get(pos);
      return ret;
    }

    public int getColOffset(int pos) {
      if(pos<0) return -1;
      int offset = getFirstFixOffset();
      int type = types.get(pos);
      int addedCol = colIsAdded.get(pos);
      if(!(type < REC_MAX_V_CHAR_H && type >= REC_MIN_V_CHAR_H))  //fixed
      {
        for(int i=0 ; i < fixedColPos.size(); i++)
        {
          int fixedPos = fixedColPos.get(i);
          if( fixedPos == pos) //this is the col
          {
            return offset;
          }
          if(addedCol == 1){
            offset = getAlignedSize(offset, i, sizes.get(fixedPos));
          } else {
            offset = offset + sizes.get(fixedPos) ;
          }
        }
      }
      else 
      {
        int voffset = getVO(pos);
        offset = bb.getInt(voffset);
      }
      return offset;
    }

    public int getKColOffset(int pos, boolean keynullable) {
      int offset = 0;
      for(int i=0; i<pos; i++)
      {
        if(keynullable == true)
        {
          if(kcolIsSystem.get(i) == 0) //not system key
            offset += 2;
          offset += ksizes.get(i);
        }
        else
          offset += ksizes.get(i);
      }
      return offset;
    }

    //parsing the key list
    private void parseKeyList(String colList) {
      if(kinitialized == true) return;
      if(colList == "syskey" ) return;
      String cols[] = colList.split(";");
      int colIdx = 0;

      for( String col : cols ) {
        kcolNum++;
        String attrs[] = col.split(" ");
        String attrsv2[] = col.split("-");
        if(attrsv2.length >= 12 ) {
          int rawType = Integer.parseInt(attrsv2[0]);
          ktypes.add(rawType);

          switch(attrsv2[1]) {
            case "0":
              knulls.add(0); //NOT NULL
              break;
            case "1":
              knulls.add(1); //NULLABLE
              break;
            default:
              knulls.add(0); //NOT NULL
              break;
          }

          //handle size
          int s = Integer.parseInt(attrsv2[2]);
          ksizes.add(s);

          //caculate the length of rowkey
          keyLength += s; 

          //precision
          kprecisions.add(Integer.parseInt(attrsv2[4]));
          //scale
          kscales.add(Integer.parseInt(attrsv2[5]));
          //charset
          kcharsets.add(attrsv2[6]);
          //check name if it is SYSKEY, set no pk as true, this is v2 behavior
          if(attrsv2[11].equals("syskey") || attrsv2[11].equals("SYSKEY"))
          {
            setNoPk(true);
          }
          //tableType
          if(attrsv2[3].equals("S"))
            kcolIsSystem.add(1);
          else
            kcolIsSystem.add(0);
          ktablepos.add(Integer.parseInt(attrsv2[10]));

          //key order
          if(attrsv2.length > 12)  
            korders.add(Short.parseShort(attrsv2[12]));
          else
          {
            short zero = 0;
            korders.add(zero);
          }

        }
        else {
          int rawType = Integer.parseInt(attrs[0]);
          ktypes.add(rawType);
          //handle size
          int s = Integer.parseInt(attrs[2]);
          ksizes.add(s);
          //caculate the length of rowkey
          keyLength += s; 
          //precision
          kprecisions.add(Integer.parseInt(attrs[4]));
          //scale
          kscales.add(Integer.parseInt(attrs[5]));
          //charset
          kcharsets.add(attrs[6]);
          //tableType
          int s1 = Integer.parseInt(attrs[3]);
          kcolIsSystem.add(s1);
        }
      }
      kinitialized = true;
      orderedktablepos.addAll(ktablepos);
      Collections.sort(orderedktablepos);
    }

    /*
     parsing the table schema information
    */
    private void parseColList(String colList) {
      if(initialized == true) return;
      String cols[] = colList.split(";");
      int colIdx = 0;
      for( String col : cols ) {
        colNum++;
        String attrs[] = col.split(" ");
        String attrsv2[] = col.split("-");
        versionSize = attrs.length;
        versionSizev2 = attrsv2.length;
        if(versionSizev2 == 12) // this is a v2 version meta
        {
          //todo
          int rawType = Integer.parseInt(attrsv2[0]);
          types.add(rawType);
          if(rawType >= REC_MIN_V_CHAR_H && rawType < REC_MAX_V_CHAR_H)
            varColNum++;
          switch(attrsv2[1]) {
            case "0":
              nulls.add(0); //NOT NULL
              break;
            case "1":
              nulls.add(1); //NULLABLE
              break;
            default:
              nulls.add(0); //NOT NULL
              break;
          }
          //handle size
          int s = Integer.parseInt(attrsv2[2]);
          sizes.add(s);

          //tableType
          if(attrsv2[3].equals("S"))
            colIsSystem.add(1);
          else
            colIsSystem.add(0);

          //precision
          precisions.add(Integer.parseInt(attrsv2[4]));

          //scale
          scales.add(Integer.parseInt(attrsv2[5]));

          //charset
          charsets.add(attrsv2[6]);
          
          //added column now check attrsv2[3]
          if(attrsv2[3].equals("A") || attrsv2[3].equals("C") )
           colIsAdded.add(1);
          else 
           colIsAdded.add(0);

         //sql data type, text
         sqltype.add(attrsv2[7]);

         //datetime_start_field
         dts.add(Integer.parseInt(attrsv2[8]));

         //datetime_end_field
         des.add(Integer.parseInt(attrsv2[9]));

         //Now it seems no need to parse the column number

         //Column Name
         colNames.add(attrsv2[11]);
 
        } 
        else
        {
          int rawType = Integer.parseInt(attrs[0]);
          types.add(rawType);
          if(rawType >= REC_MIN_V_CHAR_H && rawType < REC_MAX_V_CHAR_H)
            varColNum++;
          switch(attrs[1]) {
            case "0":
              nulls.add(0); //NOT NULL
              break;
            case "1":
              nulls.add(1); //NULLABLE
              break;
            default:
              nulls.add(0); //NOT NULL
              break;
          }

          //handle size
          int s = Integer.parseInt(attrs[2]);
          sizes.add(s);

          //tableType
          int s1 = Integer.parseInt(attrs[3]);
          colIsSystem.add(s1);

          //precision
          precisions.add(Integer.parseInt(attrs[4]));
        
          //scale
          scales.add(Integer.parseInt(attrs[5]));

          //charset
          charsets.add(attrs[6]);

          //add column
          if(versionSize > 7)
            colIsAdded.add(Integer.parseInt(attrs[7]));
	  else
	    colIsAdded.add(0);
        }
      } 
      initialized = true;
    }
    public int orderFixedFieldsByAlignment() {
        //fisrt round for normal columns
        for(int i=0; i< colNum ; i++) {
          int type = types.get(i);
          int size = sizes.get(i);
          if(!(type < REC_MAX_V_CHAR_H && type >= REC_MIN_V_CHAR_H) )  //fixed
          {
            if(colIsAdded.size() > 0)  //from version 8, it can support added column
            {
              if(colIsAdded.get(i) == 1) //this is an added column, parse it in second round, all added column are appended at the end
                continue;
            }
            switch(type) {
              case REC_BIN32_SIGNED:
              case REC_BIN32_UNSIGNED:
              case REC_FLOAT32:
                align4.add(i);
                break;
              case REC_BIN64_SIGNED:
              case REC_BIN64_UNSIGNED:
              case REC_FLOAT64:
                align8.add(i);
                break;
              case REC_BIN16_SIGNED:
              case REC_BIN16_UNSIGNED:
              case REC_NCHAR_F_UNICODE:
              case REC_NCHAR_V_UNICODE:
              case REC_NUM_BIG_SIGNED:
              case REC_NUM_BIG_UNSIGNED:
                align2.add(i);
                break;
              default:
                align1.add(i);
                break;
            }
          }
        }

        //second round only for added columns
        if(colIsAdded.size() > 0) {
          for(int i=0; i< colNum ; i++) {
            int type = types.get(i);
            if(!(type < REC_MAX_V_CHAR_H && type >= REC_MIN_V_CHAR_H) )  //fixed
            {
              if(colIsAdded.get(i) == 0) //if this is an added column, parse it in second round, all added column are appended at the end
                continue;
              else
              {
                align1.add(i);
              }
            }
          }
        }
        for(int i=0; i< align8.size(); i++)
          fixedColPos.add(align8.get(i));
        for(int i=0; i< align4.size(); i++)
          fixedColPos.add(align4.get(i));
        for(int i=0; i< align2.size(); i++)
          fixedColPos.add(align2.get(i));
        for(int i=0; i< align1.size(); i++)
          fixedColPos.add(align1.get(i));
        return 0;
    }
     public void setTimestamp(long t) { ts_ = t; }
     public long getTimestamp() { return ts_; }
     private String rtrim(String value) {
        int len = value.length();
        int st = 0;
        char[] val = value.toCharArray();
        while ((st < len) && (val[len - 1] == ' ')) {
            len--;
        }
        return ((st > 0) || (len < value.length())) ? new String(val).substring(st, len) : new String(val);
    }

    public void setDoKeyReorder(boolean b ) { doKeyReorder = b; }
    public boolean getDoKeyReorder() { return doKeyReorder; }
}

