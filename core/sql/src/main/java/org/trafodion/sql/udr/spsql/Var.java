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

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Calendar;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Date;
import java.sql.Timestamp;

/**
 * Variable or the result of expression 
 */
public class Var {
  private static final Logger LOG = Logger.getLogger(Var.class);

  // Data types
  public enum Type {BOOL, CURSOR, DATE, DECIMAL, DERIVED_TYPE, DERIVED_ROWTYPE, DOUBLE, FILE, IDENT, BIGINT, INTERVAL, ROW, 
                    RS_LOCATOR, STRING, STRINGLIST, TIMESTAMP, TIME, LOB, NULL};
  public static final String DERIVED_TYPE = "DERIVED%TYPE";
  public static final String DERIVED_ROWTYPE = "DERIVED%ROWTYPE";
  public static Var Empty = new Var();
  public static Var Null = new Var(Type.NULL);
	
  public String name;
  public Type type; 
  public Object value;
	
  int len;
  int scale;
	
  boolean constant = false;
  boolean nullable = true;
	
  public Var() {
    type = Type.NULL;  
  }
	
  public Var(Var var) {
    name = var.name;
    type = var.type;
    value = var.value;
    len = var.len;
    scale = var.scale;
    nullable = var.nullable;
  }
	
  public Var(Long value) {
    this.type = Type.BIGINT;
    this.value = value;
  }
	
  public Var(BigDecimal value) {
    this.type = Type.DECIMAL;
    this.value = value;
    if (value != null) {
      len = value.precision();
      scale = value.scale();
    }
    LOG.trace("VAR DECIMAL"
	      + " value=" + value
	      + " len=" + len
	      + " scale=" + scale);
  }
  
  public Var(String name, Long value) {
    this.type = Type.BIGINT;
    this.name = name;    
    this.value = value;
  }
  
  public Var(String value) {
    this.type = Type.STRING;
    this.value = value;
  }
  
  public Var(Double value) {
    this.type = Type.DOUBLE;
    this.value = value;
  }
	
  public Var(Date value) {
    this.type = Type.DATE;
    this.value = value;
  }

  public Var(Time value) {
    this.type = Type.TIME;
    this.value = value;
    this.scale = 0;
  }

  public Var(Time value, int scale) {
    this.type = Type.TIME;
    this.value = value;
    if (scale > 6) {
      this.scale = 6;
    } else {
      this.scale = scale;
    }
  }

  public Var(Timestamp value, int scale) {
    this.type = Type.TIMESTAMP;
    this.value = value;
    if (scale > 6) {
      this.scale = 6;
    } else {
      this.scale = scale;
    }
  }
	
  public Var(Interval value) {
    this.type = Type.INTERVAL;
    this.value = value;
  }

  public Var(ArrayList<String> value) {
    this.type = Type.STRINGLIST;
    this.value = value;
  }
  
  public Var(Boolean b) {
    type = Type.BOOL;
    value = b;
  }
	
  public Var(String name, Row row) {
    this.name = name;
    this.type = Type.ROW;
    this.value = new Row(row);
  }
	
  public Var(Type type, String name) {
    this.type = type;
    this.name = name;
  }
  
  public Var(Type type, Object value) {
    this.type = type;
    this.value = value;
  }
  
  public Var(String name, Type type, Object value) {
    this.name = name;
    this.type = type;
    this.value = value;
  }
	
  public Var(Type type) {
    this.type = type;
  }

  public Var(String name, String type, String len, String scale, Var def) {
    LOG.trace("VAR"
              + " name=" + name
              + " type=" + type
              + " len=" + len
              + " scale=" + scale
              + " def=" + def);
    this.name = name;
    setType(type);	  
    if (len != null) {
      this.len = Integer.parseInt(len);
    }
    if (scale != null) {
      this.scale = Integer.parseInt(scale);
    }
    if (this.type == Type.TIME || this.type == Type.TIMESTAMP) {
      // TIME and TIMESTAMP does not have len, only scale
      if (len == null) {
        this.scale = (this.type == Type.TIME) ? 0 : 6;
      } else {
        this.scale = this.len;
      }
      if (this.scale > 6) {
        this.scale = 6;
      }
    }
    if (def != null) {
      cast(def);
    }
  }
	
  /**
   * Cast a new value to the variable 
   */
  public Var cast(Var val) {
    if (constant) {
      return this;
    }
    else if (val == null || val.value == null) {
      if (!nullable) {
        throw new RuntimeException("cannot assign NULL value to NOT NULL variable '" + name + "'");
      }
      value = null;
    }
    else if (type == Type.DERIVED_TYPE) {
      type = val.type;
      value = val.value;
      len = val.len;
      scale = val.scale;
    }
    else if (type == val.type && type == Type.STRING) {
      cast((String)val.value);
    }
    else if (type == val.type) {
      value = val.value;
    }
    else if (type == Type.STRING) {
      cast(val.toString());
    }
    else if (type == Type.BIGINT) {
      if (val.type == Type.STRING) {
        // String can contain the fractional part that must be discarded i.e. 10.3 -> 10
        value = new Long(new BigDecimal((String)val.value).longValue());
      }
      else if (val.type == Type.DECIMAL || val.type == Type.DOUBLE) {
        value = new Long(val.longValue());
      }
    }
    else if (type == Type.DECIMAL) {
      if (val.type == Type.STRING) {
        value = new BigDecimal((String)val.value);
      }
      else if (val.type == Type.BIGINT) {
        value = BigDecimal.valueOf(val.longValue());
      }
      else if (val.type == Type.DOUBLE) {
        value = BigDecimal.valueOf(val.doubleValue());
      }
    }
    else if (type == Type.DOUBLE) {
      if (val.type == Type.STRING) {
        value = new Double((String)val.value);
      }
      else if (val.type == Type.BIGINT || val.type == Type.DECIMAL) {
        value = Double.valueOf(val.doubleValue());
      }
    }
    else if (type == Type.DATE) {
      value = Utils.toDate(val.toString());
    }
    else if (type == Type.TIMESTAMP) {
      value = Utils.toTimestamp(val.toString());
    }
    else if (type == Type.TIME) {
      value = Utils.toTime(val.toString());
    }
    if (type == Type.DECIMAL && value != null) {
      MathContext mc = new MathContext(len, RoundingMode.DOWN);
      value = ((BigDecimal)value).round(mc).setScale(scale,
                                                     RoundingMode.DOWN);
    }
    return this;
  }

  public void setNullable(boolean v) {
    nullable = v;
    if (!nullable && value == null) {
      throw new RuntimeException("cannot assign NULL value to NOT NULL variable '" + name + "'");
    }
  }

  /**
   * Cast a new string value to the variable 
   */
  public Var cast(String val) {
    if (!constant && type == Type.STRING) {
      if (len != 0 ) {
        int l = val.length();
        if (l > len) {
          value = val.substring(0, len);
          return this;
        }
      }
      value = val;
    }
    return this;
  }
	
  /**
   * Set the new value 
   */
  public void setValue(String str) {
    if(!constant && type == Type.STRING) {
      value = str;
    }
  }
	
  public Var setValue(Long val) {
    if (!constant && type == Type.BIGINT) {
      value = val;
    }
    return this;
  }
	
  public Var setValue(Boolean val) {
    if (!constant && type == Type.BOOL) {
      value = val;
    }
    return this;
  }
	
  public void setValue(Object value) {
    if (!constant) { 
      this.value = value;
    }
  }
	
  /**
   * Set the new value from the result set
   */
  public Var setValue(ResultSet rs, ResultSetMetaData rsm, int idx) throws SQLException {
    int type = rsm.getColumnType(idx);
    if (type == java.sql.Types.CHAR || type == java.sql.Types.VARCHAR) {
      cast(new Var(rs.getString(idx)));
    }
    else if (type == java.sql.Types.INTEGER || type == java.sql.Types.BIGINT ||
             type == java.sql.Types.SMALLINT || type == java.sql.Types.TINYINT) {
      cast(new Var(new Long(rs.getLong(idx))));
    }
    else if (type == java.sql.Types.DECIMAL || type == java.sql.Types.NUMERIC) {
      cast(new Var(rs.getBigDecimal(idx)));
    }
    else if (type == java.sql.Types.FLOAT || type == java.sql.Types.DOUBLE || type == java.sql.Types.REAL) {
      cast(new Var(new Double(rs.getDouble(idx))));
    }
    else if (type == java.sql.Types.DATE) {
      cast(new Var(rs.getDate(idx)));
    }
    else if (type == java.sql.Types.TIME) {
      cast(new Var(rs.getString(idx)));
    }
    else if (type == java.sql.Types.TIMESTAMP) {
      cast(new Var(rs.getTimestamp(idx), 6));
    } else {
      throw new IllegalArgumentException();
    }
    if (rs.wasNull()) {
      if (!nullable) {
        throw new RuntimeException("cannot assign NULL value to NOT NULL variable '" + name + "'");
      }
      value = null;
    }
    return this;
  }
  
  /**
   * Set ROW values from the result set
   */
  public Var setValues(ResultSet rs, ResultSetMetaData rsm) throws SQLException {
    Row row = (Row)this.value;
    int idx = 1;
    for (Column column : row.getColumns()) {
      Var var = new Var(column.getName(), column.getType(),
                        String.valueOf(column.getLen()),
                        String.valueOf(column.getScale()), null);
      var.setValue(rs, rsm, idx);
      column.setValue(var);
      idx++;
    }
    return this;
  }
	
  /**
   * Set the data type from string representation
   */
  void setType(String type) {
    this.type = defineType(type);
  }
	
  /**
   * Set the data type from JDBC type code
   */
  void setType(int type) {
    this.type = defineType(type);
  }
  
  /**
   * Set the variable as constant
   */
  void setConstant(boolean constant) {
    this.constant = constant;
  }
    
  /**
   * Define the data type from string representation
   */
  public static Type defineType(String type) {
    if (type == null) {
      return Type.NULL;
    }
    else if (type.equalsIgnoreCase("INT") || type.equalsIgnoreCase("INTEGER") ||
	     type.equalsIgnoreCase("BIGINT") || type.equalsIgnoreCase("LARGEINT") ||
             type.equalsIgnoreCase("SMALLINT") || type.equalsIgnoreCase("TINYINT") ||
             type.equalsIgnoreCase("BINARY_INTEGER") || type.equalsIgnoreCase("PLS_INTEGER") ||
             type.equalsIgnoreCase("SIMPLE_INTEGER") || type.equalsIgnoreCase("INT2") ||
             type.equalsIgnoreCase("INT4") || type.equalsIgnoreCase("INT8")) {
      return Type.BIGINT;
    }
    else if (type.equalsIgnoreCase("CHAR") || type.equalsIgnoreCase("VARCHAR") || type.equalsIgnoreCase("VARCHAR2") ||
             type.equalsIgnoreCase("NCHAR") || type.equalsIgnoreCase("NVARCHAR") ||
             type.equalsIgnoreCase("STRING") || type.equalsIgnoreCase("XML")) {
      return Type.STRING;
    }
    else if (type.equalsIgnoreCase("DEC") || type.equalsIgnoreCase("DECIMAL") || type.equalsIgnoreCase("NUMERIC") ||
             type.equalsIgnoreCase("NUMBER")) {
      return Type.DECIMAL;
    }
    else if (type.equalsIgnoreCase("REAL") || type.equalsIgnoreCase("FLOAT") || type.toUpperCase().startsWith("DOUBLE") ||
             type.equalsIgnoreCase("BINARY_FLOAT") || type.toUpperCase().startsWith("BINARY_DOUBLE") ||
             type.equalsIgnoreCase("SIMPLE_FLOAT") || type.toUpperCase().startsWith("SIMPLE_DOUBLE")) {
      return Type.DOUBLE;
    }
    else if (type.equalsIgnoreCase("DATE")) {
      return Type.DATE;
    }
    else if (type.equalsIgnoreCase("TIME")) {
      return Type.TIME;
    }
    else if (type.equalsIgnoreCase("TIMESTAMP") || type.equalsIgnoreCase("DATETIME")) {
      return Type.TIMESTAMP;
    }
    else if (type.equalsIgnoreCase("BOOL") || type.equalsIgnoreCase("BOOLEAN")) {
      return Type.BOOL;
    }
    else if (type.equalsIgnoreCase("SYS_REFCURSOR")) {
      return Type.CURSOR;
    }
    else if (type.equalsIgnoreCase("UTL_FILE.FILE_TYPE")) {
      return Type.FILE;
    }
    else if (type.toUpperCase().startsWith("RESULT_SET_LOCATOR")) {
      return Type.RS_LOCATOR;
    }
    else if (type.equalsIgnoreCase(Var.DERIVED_TYPE)) {
      return Type.DERIVED_TYPE;
    }
    throw new RuntimeException("The '" + type + "' data type is unknown");
  }
  
  /**
   * Define the data type from JDBC type code
   */
  public static Type defineType(int type) {
    if (type == java.sql.Types.CHAR || type == java.sql.Types.VARCHAR) {
      return Type.STRING;
    }
    else if (type == java.sql.Types.INTEGER || type == java.sql.Types.BIGINT) {
      return Type.BIGINT;
    }
    return Type.NULL;
  }
	
  /**
   * Remove value
   */
  public void removeValue() {
    type = Type.NULL;  
    name = null;
    value = null;
    len = 0;
    scale = 0;
  }
	
  /**
   * Compare values
   */
  @Override
  public boolean equals(Object obj) {
    return equals(obj, false);
  }

  public boolean equals(Object obj, boolean emptyIsNull) {
    if (getClass() != obj.getClass()) {
      return false;
    }    
    Var var = (Var)obj;  
    if (this == var) {
      return true;
    }
    else if (var == null) {
      return false;
    }
    else if (var.value == null || this.value == null) {
      // Treat EMPTY string as NULL
      return (emptyIsNull &&
              type == Type.STRING &&
              var.type == Type.STRING &&
              (var.value == null || ((String)var.value).equals("")) &&
              (this.value == null || ((String)this.value).equals("")));
    }
    if (isNumeric() || var.isNumeric()) {
      try {
	return (compareTo(var, emptyIsNull) == 0);
      } catch (NumberFormatException e) {
	return false;
      }
    }
    else if (type == Type.STRING && var.type == Type.STRING) {
      if (((String)value).equals((String)var.value)) {
        return true;
      }
      // Treat EMPTY string as NULL
      if (emptyIsNull &&
          (var.value == null || ((String)var.value).equals("")) &&
          (this.value == null || ((String)this.value).equals(""))) {
        return true;
      }
    }
    else if (type != var.type) {
      return false;
    } else {
      return value.equals(var.value);
    }
    return false;
  }
    
  public boolean isNumeric() {
    return (type == Type.BIGINT  ||
            type == Type.DECIMAL ||
            type == Type.DOUBLE);
  }

  /**
   * Compare values
   */
  public int compareTo(Var v, boolean emptyIsNull) {
    if (this == v) {
      return 0;
    }
    else if (v == null) {
      return -1;
    }
    else if (type == Type.BIGINT && v.type == Type.BIGINT) {
      return Long.compare(longValue(), v.longValue());
    }
    else if (type == Type.DOUBLE && v.type == Type.DOUBLE) {
      return Double.compare(doubleValue(), v.doubleValue());
    }
    else if (isNumeric() || v.isNumeric()) {
      return decimalValue().compareTo(v.decimalValue());
    }
    else if (type == Type.STRING && v.type == Type.STRING) {
      // Treat empty string as null
      if (emptyIsNull &&
          (value == null || ((String)value).equals("")) &&
          (v.value == null || ((String)v.value).equals(""))) {
        return 0;
      }
      return ((String)value).compareTo((String)v.value);
    }
    return -1;
  }
  
  /**
   * Calculate difference between values in percent
   */
  public BigDecimal percentDiff(Var var) {
    BigDecimal d1 = new Var(Var.Type.DECIMAL).cast(this).decimalValue();
    BigDecimal d2 = new Var(Var.Type.DECIMAL).cast(var).decimalValue();
    if (d1 != null && d2 != null) {
      if (d1.compareTo(BigDecimal.ZERO) != 0) {
        return d1.subtract(d2).abs().multiply(new BigDecimal(100)).divide(d1, 2, RoundingMode.HALF_UP);
      }
    }
    return null;
  }

  /**
   * Compare two variable data types for compatibility.
   */
  public static Boolean compatibleType(Var.Type type1, Var.Type type2) {
    if (type1.equals(type2)) {
      return true;
    } else if (type1.equals(Var.Type.DECIMAL) && (type2.equals(Var.Type.BIGINT) || type2.equals(Var.Type.DOUBLE))) {
      return true;
    } else{
      return false;
    }
  }

  /**
   * Increment an integer value
   */
  public Var increment(Long i) {
    if (type == Type.BIGINT) {
      value = new Long(((Long)value).longValue() + i);
    }
    return this;
  }

  /**
   * Decrement an integer value
   */
  public Var decrement(Long i) {
    if (type == Type.BIGINT) {
      value = new Long(((Long)value).longValue() - i);
    }
    return this;
  }
  
  /**
   * Return an integer value
   */
  public int intValue() {
    if (type == Type.BIGINT) {
      return ((Long)value).intValue();
    }
    else if (type == Type.DECIMAL) {
      return ((BigDecimal)value).intValue();
    }
    else if (type == Type.DOUBLE) {
      return ((Double)value).intValue();
    }
    else if (type == Type.STRING) {
      return Integer.parseInt(((String)value).trim());
    }
    throw new NumberFormatException();
  }
	
  /**
   * Return a long integer value
   */
  public long longValue() {
    if (type == Type.BIGINT) {
      return ((Long)value).longValue();
    }
    else if (type == Type.DECIMAL) {
      return ((BigDecimal)value).longValue();
    }
    else if (type == Type.DOUBLE) {
      return ((Double)value).longValue();
    }
    else if (type == Type.STRING) {
      return Long.parseLong(((String)value).trim());
    }
    throw new NumberFormatException();
  }
  
  /**
   * Return a decimal value
   */
  public BigDecimal decimalValue() {
    if (type == Type.DECIMAL) {
      return (BigDecimal)value;
    }
    else if (type == Type.BIGINT) {
      return BigDecimal.valueOf(((Long)value).longValue());
    }
    else if (type == Type.DOUBLE) {
      return new BigDecimal(toString());
    }
    else if (type == Type.STRING) {
      return new BigDecimal(((String)value).trim());
    }
    throw new NumberFormatException();
  }
  
  /**
   * Return a double value
   */
  public double doubleValue() {
    if (type == Type.DOUBLE) {
      return ((Double)value).doubleValue();
    }
    else if (type == Type.BIGINT) {
      return ((Long)value).doubleValue();
    }
    else if (type == Type.DECIMAL) {
      return ((BigDecimal)value).doubleValue();
    }
    else if (type == Type.STRING) {
      return Double.parseDouble(((String)value).trim());
    }
    throw new NumberFormatException();
  }
	
  /**
   * Return true/false for BOOL type
   */
  public boolean isTrue() {
    if(type == Type.BOOL && value != null) {
      return ((Boolean)value).booleanValue();
    }
    return false;
  }
	
  /**
   * Negate the boolean value
   */
  public void negate() {
    if(type == Type.BOOL && value != null) {
      boolean v = ((Boolean)value).booleanValue();
      value = Boolean.valueOf(!v);
    }
  }
	
  /**
   * Check if the variable contains NULL
   */
  public boolean isNull(boolean emptyIsNull) {
    if (type == Type.NULL || value == null) {
      return true;
    }
    // Treat EMPTY string as NULL
    if (emptyIsNull &&
        type == Type.STRING &&
        ((String)value).equals("")) {
      return true;
    }
    return false;
  }
	
  public int getYear() {
    if (type == Type.INTERVAL) {
      return ((Interval)value).getYear();
    }
    if (type == Type.DATE ||
        type == Type.TIMESTAMP) {
      return toCalendar().get(Calendar.YEAR);
    }
    throw new IllegalArgumentException();
  }

  public int getMonth() {
    if (type == Type.INTERVAL) {
      return ((Interval)value).getMonth();
    }
    if (type == Type.DATE ||
        type == Type.TIMESTAMP) {
      return toCalendar().get(Calendar.MONTH) + 1;
    }
    throw new IllegalArgumentException();
  }

  public int getDay() {
    if (type == Type.INTERVAL) {
      return ((Interval)value).getDay();
    }
    if (type == Type.DATE ||
        type == Type.TIMESTAMP) {
      return toCalendar().get(Calendar.DAY_OF_MONTH);
    }
    throw new IllegalArgumentException();
  }

  public int getHour() {
    if (type == Type.INTERVAL) {
      return ((Interval)value).getHour();
    }
    if (type == Type.TIME ||
        type == Type.TIMESTAMP) {
      return toCalendar().get(Calendar.HOUR_OF_DAY);
    }
    throw new IllegalArgumentException();
  }

  public int getMinute() {
    if (type == Type.INTERVAL) {
      return ((Interval)value).getMinute();
    }
    if (type == Type.TIME ||
        type == Type.TIMESTAMP) {
      return toCalendar().get(Calendar.MINUTE);
    }
    throw new IllegalArgumentException();
  }

  public int getSecond() {
    if (type == Type.INTERVAL) {
      return ((Interval)value).getSecond();
    }
    if (type == Type.TIME ||
        type == Type.TIMESTAMP) {
      return toCalendar().get(Calendar.SECOND);
    }
    throw new IllegalArgumentException();
  }

  /**
   * Convert value to Calendar
   */
  public Calendar toCalendar() {
    if (value == null) {
      throw new IllegalArgumentException("NULL value not allowed");
    }
    if (type != Type.DATE && type != Type.TIME && type != Type.TIMESTAMP) {
      throw new IllegalArgumentException("DATE/TIME/TEMESTAMP data type required");
    }
    long millis = 0;
    if (type == Type.DATE) {
      millis = ((Date)value).getTime();
    } else if (type == Type.TIME) {
      millis = ((Time)value).getTime();
    } else if (type == Type.TIMESTAMP) {
      millis = ((Timestamp)value).getTime();
    }
    Calendar cal = Calendar.getInstance();
    cal.setTimeInMillis(millis);
    return cal;
  }

  /**
   * Convert value to String
   */
  @Override
  public String toString() {
    if (type == Type.IDENT) {
      return name;
    }   
    else if (value == null) {
      return null;
    }
    else if (type == Type.BIGINT) {
      return ((Long)value).toString();
    }
    else if (type == Type.STRING) {
      return (String)value;
    }
    else if (type == Type.DATE) {
      return ((Date)value).toString();
    }
    else if (type == Type.TIME) {
      int len = 8;
      String t = ((Time)value).toString();   // .0 returned if the fractional part not set
      if (scale > 0) {
        len += scale + 1;
      }
      if (t.length() > len) {
        t = t.substring(0, len);
      }
      while (t.length() < len) {
        t += "0";
      }
      return t;
    }
    else if (type == Type.TIMESTAMP) {
      int len = 19;
      String t = ((Timestamp)value).toString();   // .0 returned if the fractional part not set
      if (scale > 0) {
        len += scale + 1;
      }
      if (t.length() > len) {
        t = t.substring(0, len);
      }
      while (t.length() < len) {
        t += "0";
      }
      return t;
    } else if (type == Type.DECIMAL) {
      return ((BigDecimal)value).stripTrailingZeros().toPlainString();
    }
    return value.toString();
  }

  /**
   * Convert value to SQL string - string literals are quoted and escaped, ab'c -> 'ab''c'
   */
  public String toSqlString() {
    if (value == null) {
      return "NULL";
    }
    else if (type == Type.STRING) {
      return Utils.quoteString((String)value);
    }
    else if (type == Type.DATE || type == Type.TIME || type == Type.TIMESTAMP) {
      return type + " " + Utils.quoteString(toString());
    }
    else if (type == Type.ROW) {
      // Build a comma separated value of all columns
      StringBuilder sb = new StringBuilder();
      for (Column col : ((Row)value).getColumns()) {
        if (sb.length() > 0) {
          sb.append(",");
        }
        sb.append(col.getValue().toSqlString());
      }
      return sb.toString();
    }
    return toString();
  }
	
  /**
   * Set variable name
   */
  public void setName(String name) {
    this.name = name;
  }
  
  /**
   * Get variable name
   */
  public String getName() {
    return name;
  }

  public void setLen(int len) {
    this.len = len;
  }

  public void setScale(int scale) {
    this.scale = scale;
  }
}
