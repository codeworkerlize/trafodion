package org.trafodion.jdbc.t4.function;

public class CacheFun {

  public static Integer to_number(String value) {
    if (value == null || "".equals(value.trim())) {
      return null;
    }
    return Integer.valueOf(value.trim());
  }
}
