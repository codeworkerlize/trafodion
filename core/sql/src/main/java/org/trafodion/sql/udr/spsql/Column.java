/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.trafodion.sql.udr.spsql;

/**
 * Table column
 */
public class Column {
  public static final String CHARACTER_SET_UCS2 = "UCS2";
  public static final String CHARACTER_SET_KSC5601_MP = "KSC5601_MP";
  public static final String CHARACTER_SET_KANJI_MP = "KANJI_MP";
  public static final int MAX_HARDWARE_SUPPORTED_SIGNED_NUMERIC_PRECISION = 18;
  public static final int MAX_HARDWARE_SUPPORTED_UNSIGNED_NUMERIC_PRECISION = 20;

  
  String name;
  String type;
  int fsDataType = -1;
  Var value;
  
  int len;
  int precision;
  int scale;
  String characterSet;
  int dateTimeStartField;
  int dateTimeEndField;

  //row data order flag
  int hbaseDataAligement;

  boolean isNull;
  //column data is null
  boolean valueNull;
  String defaultValue;
  /*
   * column default value, now we only support COM_NULL_DEFAULT(2)
   * and COM_USER_DEFINED_DEFAULT(3), 
   * reference enum ComColumnDefaultClass in ComSmallDefs.h
   */
  int defaultClass;
  boolean isAddCol;
  int nullColIndex;

  
  Column(String name, String type, int len, int scale) {
    this.name = name;
    this.len = len;
    this.scale = scale;
    setType(type);
  }

  Column(String name, String type, int fsDataType,
	 int len, int precision, int scale,
	 boolean isNull, String characterSet,
	 int dateTimeStartField, int dateTimeEndField) {
    this.name = name;
    this.len = len;
    this.precision = precision;
    this.scale = scale;
    this.fsDataType = fsDataType;
    this.isNull = isNull;
    this.characterSet = characterSet;
    this.dateTimeStartField = dateTimeStartField;
    this.dateTimeEndField = dateTimeEndField;
    setType(type);
    hbaseDataAligementCalc();
    if (type.equals("UNSIGNED TINYINT") && precision == 1) {
        hbaseDataAligement = 1;
    }
    setNumericAligement();
  }

  Column(String name, String type, int fsDataType,
	 int len, int precision, int scale,
	 boolean isNull, String characterSet,
	 int dateTimeStartField, int dateTimeEndField,
         String defaultValue, int defaultClass, boolean isAddCol) {
      this(name, type, fsDataType,
             len, precision, scale,
             isNull, characterSet,
             dateTimeStartField, dateTimeEndField);
      this.defaultValue = defaultValue;
      this.defaultClass = defaultClass;
      this.isAddCol = isAddCol;
  }
    
  void setNumericAligement() {
      // if numeric convert tinyint, the hbaseDataAligement is 1,
      // fsDataType = 136 is SIGNED TINYINT or UNSIGNED TINYINT
      if ((type.equals("SIGNED NUMERIC") || type.equals("UNSIGNED NUMERIC")) ||
          (fsDataType == 136 && (precision != 0 || scale != 0))) {
          if (precision < 3) {
              hbaseDataAligement = 1;
          } else if (precision < 5 ||
                     (precision > MAX_HARDWARE_SUPPORTED_SIGNED_NUMERIC_PRECISION ||
                      (precision > MAX_HARDWARE_SUPPORTED_UNSIGNED_NUMERIC_PRECISION && type.startsWith("UNSIGNED")))) {
              hbaseDataAligement = 2;
          } else if (precision < 10) {
              hbaseDataAligement = 4;
          } else {
              hbaseDataAligement = 8;
          }
      }
      
  }
  void hbaseDataAligementCalc() {
    switch (type) {
    case "SIGNED SMALLINT":
    case "UNSIGNED SMALLINT":
    case "SIGNED TINYINT" :
    case "UNSIGNED TINYINT" :
      this.hbaseDataAligement = 2;
      break;
    case "INTERVAL" :
      this.hbaseDataAligement = len;
      break;
    case "SIGNED INTEGER" :
    case "UNSIGNED INTEGER" :
    case "REAL":
      this.hbaseDataAligement = 4;
      break;
    case "DOUBLE" :
    case "SIGNED LARGEINT" :
    case "UNSIGNED LARGEINT" :
      this.hbaseDataAligement = 8;
      break;
    case "UNSIGNED NUMERIC" :
    case "SIGNED NUMERIC" :
      if(precision > 18 ||
	 (precision > 20 && type.startsWith("UNSIGNED"))) {
	this.hbaseDataAligement = 2;
      } else {
	this.hbaseDataAligement = len;
      }
      break;
    case "DATETIME" :
      this.hbaseDataAligement = 1;
      break;
    case "SIGNED DECIMAL" :
    case "UNSIGNED DECIMAL" :
      this.hbaseDataAligement = 1;
      break;
    case "CHARACTER":
      if(CHARACTER_SET_UCS2.equals(characterSet)
	 || CHARACTER_SET_KSC5601_MP.equals(characterSet)
	 || CHARACTER_SET_KANJI_MP.equals(characterSet)) {
	this.hbaseDataAligement = 2;
      } else {
	this.hbaseDataAligement = 1;
      }
      break;
    case "VARCHAR" :
      this.hbaseDataAligement = 0;
      break;
    case "BLOB":
    case "CLOB":
      this.hbaseDataAligement = 0;
      break;
    default:
      break;
    }
  }
  
  /**
   * Set the column type with its length/precision
   */
  void setType(String type) {
    int open = type.indexOf('(');
    if (open == -1) {
      this.type = type;
    }
    else {
      this.type = type.substring(0, open);
      int comma = type.indexOf(',', open);
      int close = type.indexOf(')', open);
      if (comma == -1) {
        len = Integer.parseInt(type.substring(open + 1, close));
      }
      else {
        len = Integer.parseInt(type.substring(open + 1, comma));
        scale = Integer.parseInt(type.substring(comma + 1, close));
      }
    }
  }
  
  /**
   * Set the column value
   */
  void setValue(Var value) {
    this.value = value;
  }

  /**
   * Get the column name
   */
  String getName() {
    return name;
  }
  
  /**
   * Get the column type
   */
  String getType() {
    return type;
  }
    
  /**
   * Get the column value
   */
  Var getValue() {
    return value;
  }

  /**
   * Get the column len
   */
  int getLen() {
    return len;
  }

  /**
   * Get the column scale
   */
  int getScale() {
    return scale;
  }

  public int getPrecision() {
    return precision;
  }

  public int getHbaseDataAligement() {
    return hbaseDataAligement;
  }

  public int getFsDataType() {
    return fsDataType;
  }

  public void setFsDataType(int fsDataType) {
    this.fsDataType = fsDataType;
  }

  public boolean isNull() {
    return isNull;
  }

  public String getCharacterSet() {
    return characterSet;
  }

  public int getDateTimeStartField() {
    return dateTimeStartField;
  }

  public int getDateTimeEndField() {
    return dateTimeEndField;
  }

  public boolean isValueNull() {
    return valueNull;
  }

  public void setValueNull(boolean dataNull) {
    this.valueNull = dataNull;
  }

  public void setDefaultValue(String defaultValue) {
    this.defaultValue = defaultValue;
  }

  public String getDefaultValue() {
    return defaultValue;
  }

  public void setDefaultClass(int defaultClass) {
    this.defaultClass = defaultClass;
  }

  public int getDefaultClass() {
    return defaultClass;
  }

  public void setAddCol(boolean isAddCol) {
    this.isAddCol = isAddCol;
  }

  public boolean getAddCol() {
    return isAddCol;
  }

  public void setNullColIndex(int index)
  {
    this.nullColIndex = index;
  }

  public int getNullColIndex()
  {
    return nullColIndex;
  }
}
