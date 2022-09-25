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

package org.trafodion.sql;

import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.ArrayList;
import java.io.IOException;
import java.util.Collections;
import java.sql.Timestamp;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

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
    private List<String> kcolNames = new ArrayList<String>();
    private List<Integer> precisions = new ArrayList<Integer>();
    private List<Integer> kprecisions = new ArrayList<Integer>();
    private List<Integer> scales = new ArrayList<Integer>();
    private List<Integer> kscales = new ArrayList<Integer>();
    private List<Integer> vcLens = new ArrayList<Integer>();
    private List<Integer> sizes = new ArrayList<Integer>();
    private List<Integer> ksizes = new ArrayList<Integer>();
    private List<Integer> colIsSystem = new ArrayList<Integer>();
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

    private boolean initialized = false;
    private boolean kinitialized = false;
    private boolean checkNull = false;

    private boolean noPk = false;

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

    //constructor
    public AlignedFormatTupleParser()  
    {
    }

    public void setNoPk(boolean v) { noPk = v; }
    public boolean getNoPk() { return noPk ; }

    public void setDelimeter(String d) { delimiter = d; }

    //used to parse the primary key
    public byte[] flipFirstBit(byte[] data)
    {
      byte ccc = (byte)0x80;
      data[0] = (byte)(data[0] ^ ccc);
      return data;
    }

    public short getFF()
    {
      byte ccc = 0xC;
      FFbytes[1] = (byte)(FFbytes[1] & ccc);
      ByteBuffer ffb = ByteBuffer.wrap(FFbytes).order(ByteOrder.LITTLE_ENDIAN); 
      short v1 = ffb.getShort(0);
      return v1;
    }

    public int getFirstFixOffset()
    {
      short ff = getFF();
      return (int)ff;
    }

    public int getBitmapOffset()
    {
      return bb.getInt(FF_BO_OFFSET);
    }

    private int getAlignedSize(int size) {
      int nbytes = size / 4;
      int pad = size - 4*nbytes;
      return size + pad;
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

    public void setRawData(byte[] rawData)
    {
        bb = ByteBuffer.wrap(rawData).order(ByteOrder.LITTLE_ENDIAN);
        rawdata_ = new byte[rawData.length];
        System.arraycopy(rawData, 0, rawdata_ , 0, rawData.length);
        populateNullBitmap(rawData);
    }

    public String getKey(byte[] rk)
    {
        bb = ByteBuffer.wrap(rk);
        String part = "";
        resultString = "";

        for(int i=0; i< kcolNum ; i++)
        {
           if(i > 0 )
            resultString = resultString + ",";
           int type = ktypes.get(i);
           int colPos = getKColOffset(i);
           int len = ksizes.get(i);
             switch(type) {
               case REC_BIN16_SIGNED:
                 byte[] shortd = new byte[2];
                 System.arraycopy(rk, 0 , shortd, 0, 2);
                 shortd = flipFirstBit(shortd);
                 ByteBuffer shortbb = ByteBuffer.wrap(shortd);
                 short sv = shortbb.getShort(0);
                 part = Short.toString(sv);
                 break;
               case REC_BIN16_UNSIGNED:
                 ByteBuffer ushortbb = ByteBuffer.wrap(rk);
                 int usv = ushortbb.getInt(0) & 0xffff;
                 part = Integer.toString(usv);
                 break;
               case REC_BIN32_SIGNED:
                 byte[] intd = new byte[4];
                 System.arraycopy(rk, 0 , intd, 0, 4);
                 intd = flipFirstBit(intd);
                 ByteBuffer intbb = ByteBuffer.wrap(intd);
                 int iv = intbb.getInt(0);
                 part = Integer.toString(iv);
                 break;
               case REC_BIN32_UNSIGNED:
                 ByteBuffer uintbb = ByteBuffer.wrap(rk);
                 long uiv = uintbb.getInt(0) & 0xffffffffL;
                 part = Long.toString(uiv);
                 break;
               case REC_BIN64_SIGNED:
                 byte[] longd = new byte[8];
                 System.arraycopy(rk, 0 , longd, 0, 8);
                 longd = flipFirstBit(longd);
                 ByteBuffer longbb = ByteBuffer.wrap(longd);
                 long lv = longbb.getLong(0);
                 part = Long.toString(lv) ;
                 break;
               case REC_BIN64_UNSIGNED:
                 ByteBuffer ulongbb = ByteBuffer.wrap(rk);
                 long ulv = ulongbb.getLong(0) & 0xffffffffL;
                 part = Long.toString(ulv) ;
                 break;
               case REC_BYTE_V_ASCII:
               case REC_VARBINARY_STRING:
               case REC_BYTE_F_ASCII:
                 String sf = getStringFromBB(colPos, len);
                 part = "'" + sf + "'";
                 break;
               default:
                 part = "NotSupportedYet";
                 break;
             }
           resultString = resultString + part;

           part = "";
        }

        return resultString;
    }

    private String getDate(int pos) {
      int y = rawdata_[pos] & 0xFF | (rawdata_[pos+1]  & 0xFF ) << 8;
      byte m = rawdata_[pos+2];
      byte d = rawdata_[pos+3];
      return String.format("%d-%02d-%02d", y,m,d);
    }

    private String getTimestamp(int pos , int scale) {
      //11 bytes
      int y = rawdata_[pos] & 0xFF | (rawdata_[pos+1]  & 0xFF ) << 8;
      byte m = rawdata_[pos+2];
      byte d = rawdata_[pos+3];
      byte h = rawdata_[pos+4];
      byte mm = rawdata_[pos+5];
      byte s = rawdata_[pos+6];
      int ms = bb.getInt(pos+7);
      if( scale == 6)
        return String.format("%d-%02d-%02d %02d:%02d:%02d.%06d", y,m,d,h,mm,s,ms);
      else
        return String.format("%d-%02d-%02d %02d:%02d:%02d.%03d", y,m,d,h,mm,s,ms);

    }

    public String getNumeric(long v, int p, int s) {
      int power = 1;
      for(int i=0 ; i < s; i++)
        power = power * 10;
      long dv1 = v / power;
      long dv2 = v - dv1 * power;
      return Long.toString(dv1)+"."+Long.toString(dv2);
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
             switch(type) {
               case REC_BIN16_SIGNED:
                 short shortv = bb.getShort(colPos);
                 part = Short.toString(shortv);
                 break;
               case REC_BIN16_UNSIGNED:
                 int usv = bb.getInt(colPos) & 0xffff;
                 part = Integer.toString(usv);
                 break;
               case REC_BIN32_SIGNED:
                 int iv = bb.getInt(colPos);
                 part = Integer.toString(iv);
                 break;
               case REC_BIN32_UNSIGNED:
                 long ulv = bb.getInt(colPos) & 0xffffffffL;
                 part = Long.toString(ulv);
                 break;
               case REC_BIN64_UNSIGNED:
                 long ulv64 = bb.getLong(colPos) & 0xffffffffL;
                 part = Long.toString(ulv64) ;
                 break;
               case REC_BIN64_SIGNED:
                 if(scale == 0) {
                   long lv = bb.getLong(colPos);
                   part = Long.toString(lv) ;
                 }
                 else
                 {
                   long lv = bb.getLong(colPos);
                   part = getNumeric(lv, p, scale);
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
                 String sv = getStringFromBB(colPos, len);
                 part = "'" + sv + "'";
                 break;
               case REC_BYTE_F_ASCII:
                 String sf = getStringFromBB(colPos, len);
                 part = "'" + sf + "'";
                 break;
               case REC_DATETIME:
                 if(len == 4) //DATE
                 {
                   part = "'" +  getDate(colPos) + "'";
                 }
                 else if(len == 11) //TIMESTAMP(6)
                 {
                   part = "'" + getTimestamp(colPos, scale) + "'";
                 }
                 else
                   part = "NotSupportedYet";
                 break;
               default:
                 part = "NotSupportedYet";
                 break; 
             }
           }
           else
             part = "NULL";

           if(i < colNum -1)
            part = part + delimiter;
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
      if(!(type < REC_MAX_V_CHAR_H && type >= REC_MIN_V_CHAR_H))  //fixed
      {
        for(int i=0 ; i < fixedColPos.size(); i++)
        {
          int fixedPos = fixedColPos.get(i);
          if( fixedPos == pos) //this is the col
            return offset;
          offset = offset + sizes.get(fixedPos);
        }
      }
      else 
      {
        int voffset = getVO(pos);
        offset = bb.getInt(voffset);
      }
      return offset;
    }

    public int getKColOffset(int pos) {
      int offset = 0;
      for(int i=0; i<pos; i++)
      {
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
        int rawType = Integer.parseInt(attrs[0]);
        ktypes.add(rawType);
        //handle size
        int s = Integer.parseInt(attrs[2]);
        ksizes.add(s);
        //precision
        kprecisions.add(Integer.parseInt(attrs[4]));
        //scale
        kscales.add(Integer.parseInt(attrs[5]));
        //charset
        kcharsets.add(attrs[6]);
        //colName
        kcolNames.add(attrs[6]);
      }
      kinitialized = true;
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

      } 
      initialized = true;
    }
    public int orderFixedFieldsByAlignment() {
        for(int i=0; i< colNum ; i++) {
          int type = types.get(i);
          if(!(type < REC_MAX_V_CHAR_H && type >= REC_MIN_V_CHAR_H))  //fixed
          {
            switch(type) {
              case REC_BIN32_SIGNED:
                align4.add(i);
                break;
              case REC_BIN64_SIGNED:
                align8.add(i);
                break;
              case REC_BIN16_SIGNED:
                align2.add(i);
                break;
              default:
                align1.add(i);
                break;
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

}

