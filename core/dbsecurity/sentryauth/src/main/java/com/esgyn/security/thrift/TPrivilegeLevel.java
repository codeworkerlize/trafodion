/**
 * Autogenerated by Thrift Compiler (0.9.0)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package com.esgyn.security.thrift;


import java.util.Map;
import java.util.HashMap;
import org.apache.thrift.TEnum;

public enum TPrivilegeLevel implements org.apache.thrift.TEnum {
  ALL(0),
  INSERT(1),
  SELECT(2);

  private final int value;

  private TPrivilegeLevel(int value) {
    this.value = value;
  }

  /**
   * Get the integer value of this enum value, as defined in the Thrift IDL.
   */
  public int getValue() {
    return value;
  }

  /**
   * Find a the enum type by its integer value, as defined in the Thrift IDL.
   * @return null if the value is not found.
   */
  public static TPrivilegeLevel findByValue(int value) { 
    switch (value) {
      case 0:
        return ALL;
      case 1:
        return INSERT;
      case 2:
        return SELECT;
      default:
        return null;
    }
  }
}
