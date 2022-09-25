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

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.sql.DataTruncation;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * This class contains a variety of methods for doing all sorts of things. 
 * @version 1.0
 */

class Utility {

	private static final byte key[] = Utility.UnicodeToAscii("ci4mg04-3;" + "b,hl;y'd1q" + "x8ngp93nGp" + "oOp4HlD7vm"
			+ ">o(fHoPdkd" + "khp1`gl0hg" + "qERIFdlIFl" + "w48fgljksg" + "3oi5980rfd" + "4t8u9dfvkl");

	// -------------------------------------------------------------
	/**
	 * This method will translate a double byte Unicode string into a single
	 * byte ASCII array.
	 * 
	 * @param original
	 *            the original string
	 * 
	 * @return a byte array containing the translated string
	 * 
	 * @exception An
	 *                UnsupportedEncodingException is thrown
	 */
	static byte[] UnicodeToAscii(String original) {
		try {
			byte[] utf8Bytes = original.getBytes("UTF8");
			return utf8Bytes;
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		}
		return null;
	} // end UnicodeToAscii

	// -------------------------------------------------------------
	/**
	 * This method will encrypt a byte buffer according to the encryption
	 * algorithm used by the ODBC server.
	 * 
	 * @param original
	 *            the original string
	 * 
	 * @return a byte array containing the translated string
	 * 
	 */
	static boolean Encryption(byte inBuffer[], byte outBuffer[], int inLength) {
		// Use simple encryption/decryption

		if (outBuffer != inBuffer) {
			System.arraycopy(outBuffer, 0, inBuffer, 0, inLength);
		} // end if

		for (int i = 0; i < inLength; ++i) {
			int j = i % 100;
			outBuffer[i] ^= key[j];
		}

		return true;
	} // end Encryption

	// -------------------------------------------------------------
	/**
	 * This method will check a float value according to the MAX_FLOAT and
	 * MIN_FLOAT values in the Java language.
	 * 
	 * @param the
	 *            original double value to check
	 * @Locale the Locale to print the error message in
	 * 
	 * @return none
	 * 
	 */
	static void checkFloatBoundary(T4Properties t4Properties, BigDecimal inbd) throws SQLException {
		BigDecimal bd = new BigDecimal(String.valueOf(Float.MAX_VALUE));
		// double abdbl = inbd.abs().doubleValue(); Need to do MIN check as well
		if (inbd.compareTo(bd) > 0 ) {
			throw TrafT4Messages.createSQLException(t4Properties, "numeric_out_of_range", inbd.toString());
		}
	} // end checkFloatBoundary

	// -------------------------------------------------------------
	/**
	 * This method will check a double value according to the MAX_VALUE and
	 * MIN_VALUE values in the Double class.
	 * 
	 * @param the
	 *            original double value to check
	 * @Locale the Locale to print the error message in
	 * 
	 * @return none
	 * 
	 */
	static void checkDoubleBoundary(T4Properties t4Properties, BigDecimal inbd) throws SQLException {
		BigDecimal maxbd = new BigDecimal(Double.MAX_VALUE);
		// need to check min as well
		BigDecimal minbd = new BigDecimal(-Double.MAX_VALUE);
		if ((inbd.compareTo(maxbd) > 0) || (inbd.compareTo(minbd) < 0)) {
			throw TrafT4Messages.createSQLException(t4Properties, "numeric_out_of_range", inbd.toString());
		}

	} // end checkDoubleBoundary

	// -------------------------------------------------------------
	/**
	 * This method will check a Integer value according to the
	 * Interger.MAX_VALUE and Integer.MIN_VALUE values.
	 * 
	 * @param the
	 *            original long value to check
	 * @Locale the Locale to print the error message in
	 * 
	 * @return none
	 * 
	 */
	static void checkIntegerBoundary(T4Properties t4Properties, BigDecimal inbd) throws SQLException {
		long inlong = inbd.longValue();
		if ((inlong > Integer.MAX_VALUE) || (inlong < Integer.MIN_VALUE)) {
			throw TrafT4Messages.createSQLException(t4Properties, "numeric_out_of_range", String.valueOf(inlong));
		}
	} // end checkIntegerBoundary

	// -------------------------------------------------------------
	/**
	 * This method will check a Long value according to the Long.MAX_VALUE*2 and
	 * 0 values.
	 * 
	 * @param the
	 *            original BigDecimal value to check
	 * @Locale the Locale to print the error message in
	 * 
	 * @return none
	 * 
	 */
	static void checkUnsignedLongBoundary(T4Properties t4Properties, BigDecimal inbd) throws SQLException {
		BigDecimal maxbd = new BigDecimal(Long.MAX_VALUE);
		maxbd = maxbd.add(maxbd).add(BigDecimal.valueOf(1));
		if ((inbd.compareTo(BigDecimal.valueOf(0)) < 0) || (inbd.compareTo(maxbd) > 0)) {
			throw TrafT4Messages.createSQLException(t4Properties, "numeric_out_of_range", String.valueOf(inbd));
		}
	} // end checkIntegerBoundary

	// -------------------------------------------------------------
	/**
	 * This method will check a unsigned Short value according to the
	 * Short.MAX_VALUE*2 and 0 values.
	 * 
	 * @param the
	 *            original BigDecimal value to check
	 * @Locale the Locale to print the error message in
	 * 
	 * @return none
	 * 
	 */
	static void checkUnsignedShortBoundary(T4Properties t4Properties, BigDecimal inbd) throws SQLException {
		long inlong = inbd.longValue();
		long maxushort = (Short.MAX_VALUE * 2) + 1;
		if ((inlong < 0) || (inlong > maxushort)) {
			throw TrafT4Messages.createSQLException(t4Properties, "numeric_out_of_range", String.valueOf(inlong));
		}
	} // end checkIntegerBoundary

	// -------------------------------------------------------------
	/**
	 * This method will check a unsigned Int value according to the
	 * Integer.MAX_VALUE*2 and 0 values.
	 * 
	 * @param the
	 *            original BigDecimal value to check
	 * @Locale the Locale to print the error message in
	 * 
	 * @return none
	 * 
	 */
	static void checkUnsignedIntegerBoundary(T4Properties t4Properties, BigDecimal inbd) throws SQLException {
		long inlong = inbd.longValue();
		long maxuint = ((long) Integer.MAX_VALUE * 2L) + 1L;
		if ((inlong < 0) || (inlong > maxuint)) {
			throw TrafT4Messages.createSQLException(t4Properties, "numeric_out_of_range", String.valueOf(inlong));
		}
	} // end checkIntegerBoundary

	// -------------------------------------------------------------
	/**
	 * This method will check a Tinyint value according to the Byte.MAX_VALUE
	 * and Byte.MIN_VALUE values.
	 * 
	 * @param the
	 *            original long value to check
	 * @Locale the Locale to print the error message in
	 * 
	 * @return none
	 * 
	 */
	static void checkSignedTinyintBoundary(T4Properties t4Properties, BigDecimal inbd) throws SQLException {
		long inlong = inbd.longValue();
		if ((inlong > Byte.MAX_VALUE) || (inlong < Byte.MIN_VALUE)) {
			throw TrafT4Messages.createSQLException(t4Properties, "signed_tinyint_out_of_range", String.valueOf(inlong));
		}
	} // end checkTinyintBoundary

	static void checkUnsignedTinyintBoundary(T4Properties t4Properties, BigDecimal inbd) throws SQLException {
		long inlong = inbd.longValue();
		long maxutiny = (Byte.MAX_VALUE * 2) + 1;
		if ((inlong < 0) || (inlong > maxutiny)) {
			throw TrafT4Messages.createSQLException(t4Properties, "unsigned_tinyint_out_of_range", String.valueOf(inlong));
		}
	} // end checkTinyintBoundary

	// -------------------------------------------------------------
	/**
	 * This method will check a Short value according to the Short.MAX_VALUE and
	 * Short.MIN_VALUE values.
	 * 
	 * @param the
	 *            original long value to check
	 * @Locale the Locale to print the error message in
	 * 
	 * @return none
	 * 
	 */
	static void checkShortBoundary(T4Properties t4Properties, BigDecimal inbd) throws SQLException {
		long inlong = inbd.longValue();
		if ((inlong > Short.MAX_VALUE) || (inlong < Short.MIN_VALUE)) {
			throw TrafT4Messages.createSQLException(t4Properties, "numeric_out_of_range", String.valueOf(inlong));
		}
	} // end checkShortBoundary

	// -------------------------------------------------------------
	/**
	 * This method will extract the BigDecimal value.
	 * 
	 * @param the
	 *            Locale to print the error message in
	 * @param the
	 *            original object value to extract
	 * 
	 * @return constructed BigDecimal value
	 * 
	 */
	static BigDecimal getBigDecimalValue(T4Properties t4Properties, Object paramValue) throws SQLException {
		BigDecimal tmpbd;

		if (paramValue instanceof Long) {
			tmpbd = BigDecimal.valueOf(((Long) paramValue).longValue());
		} else if (paramValue instanceof Integer) {
			tmpbd = BigDecimal.valueOf(((Integer) paramValue).longValue());
		} else if (paramValue instanceof BigDecimal) {
			tmpbd = (BigDecimal) paramValue;
		} else if (paramValue instanceof String) {
			String sVal = (String) paramValue;
			if (sVal.equals("true") == true) {
				sVal = "1";
			} else if (sVal.equals("false") == true) {
				sVal = "0";
			}
			try {
                tmpbd = new BigDecimal(sVal);
            } catch (NumberFormatException e) {
                throw TrafT4Messages.createSQLException(t4Properties, "restricted_data_type", paramValue.getClass());
            }
		} else if (paramValue instanceof Float) {
			tmpbd = new BigDecimal(paramValue.toString());
		} else if (paramValue instanceof Double) {
			try {
                tmpbd = new BigDecimal(((Double) paramValue).toString());
            } catch (NumberFormatException e) {
                throw TrafT4Messages.createSQLException(t4Properties, "double_out_of_range", paramValue);
            }
		} else if (paramValue instanceof Boolean) {
			tmpbd = BigDecimal.valueOf(((((Boolean) paramValue).booleanValue() == true) ? 1 : 0));
		} else if (paramValue instanceof Byte) {
			tmpbd = BigDecimal.valueOf(((Byte) paramValue).longValue());
		} else if (paramValue instanceof Short) {
			tmpbd = BigDecimal.valueOf(((Short) paramValue).longValue());
		} else if (paramValue instanceof Integer) {
			tmpbd = BigDecimal.valueOf(((Integer) paramValue).longValue());
			// For LOB Support SB: 10/25/2004
			/*
			 * else if (paramValue instanceof DataWrapper) tmpbd =
			 * BigDecimal.valueOf(((DataWrapper)paramValue).longValue);
			 */
		} else {
			throw TrafT4Messages.createSQLException(t4Properties, "object_type_not_supported", paramValue.getClass());
		}
		return tmpbd;
	} // end getBigDecimalValue

	// -------------------------------------------------------------
	/**
	 * This method will check a Decimal value according to the precision in the
	 * Database table.
	 * 
	 * @param the
	 *            original BigDecimal value to check
	 * @param the
	 *            Locale to print the error message in
	 * 
	 * @return none
	 * 
	 */
	static void checkDecimalBoundary(T4Properties t4Properties, BigDecimal inbd, int precision, BigDecimal oldtmpbd) throws SQLException {
		if (precision > 0) {
			BigDecimal maxbd = new BigDecimal(Math.pow(10, precision));
			BigDecimal minbd = maxbd.negate();
			if ((inbd.compareTo(maxbd) >= 0) || (inbd.compareTo(minbd) < 0)) {
				throw TrafT4Messages.createSQLException(t4Properties, "numeric_out_of_range", oldtmpbd.toString());
			}
		}
	} // end checkBigDecimalBoundary
	//---------------------------------------------------------------
	/*code change starts
	 *  MR Description:  Warnings not  being displayed  when numeric overflow occurs
	 */

	/**
	 * This method will check precision and scale value with the column in the
	 * Database table.
	 * @param tmpbd
	 *     the BigDecimal value to check
	 * @param precision
	 *     the precision to check with the BigDecimal's precision
	 * @param scale
	 *     the scale to check with the BigDecimal's scale
	 * @throws SQLException
	 */
    static void checkPrecisionAndScale(T4Properties t4Properties, BigDecimal tmpbd, int precision, int scale)
            throws SQLException {
        if (tmpbd.precision() > precision || tmpbd.scale() > scale) {
            throw TrafT4Messages.createSQLException(t4Properties, "numeric_overflow", tmpbd.toString(), precision,
                    scale);
        }
    }

	//code change ends

	// Fix_LeadingZero - AM 08/07/2006
	private static int getExtraLen(String s) {
		int extra = 0;

		// count the trailing zero
		int inx = s.indexOf(".");
		if (inx != -1) {
			int len = s.length();
			for (int i = len - 1; i > inx; i--) {
				char ch = s.charAt(i);
				if (ch != '0') {
					break;
				}
				extra++;
			}
		}
		// count for leading zero
		if (s.startsWith("0.") || s.startsWith("-0.")) {
			extra++;
		}

		return extra;
	}

	// -------------------------------------------------------------
	/**
	 * This method will check a Decimal value according to the precision in the
	 * Database table.
	 * 
	 * @param the
	 *            original BigDecimal value to check
	 * @param the
	 *            Locale to print the error message in
	 * 
	 * @return none
	 * 
	 */
	static void checkDecimalTruncation(int parameterIndex, T4Properties t4Properties, BigDecimal inbd, int precision, int scale)
			throws SQLException {
		if (precision <= 0)
			return;

		int expectedLen = precision;

		if (scale > 0)
			expectedLen = precision + 1;

		if (inbd.signum() == -1)
			expectedLen++;
		int actualLen = 0;

		// Fix_LeadingZero - AM 08/07/2006
		expectedLen += getExtraLen(inbd.toString());
		/*
		 * if( (actualLen = inbd.toString().length()) > expectedLen ){
		 * //System.out.println("Length of " + inbd.toString() + " is greater
		 * than " + precision); throw new DataTruncation(parameterIndex, true,
		 * false, actualLen, expectedLen); }
		 */
		actualLen = inbd.toString().length();
		if (precision > 0) {
			BigDecimal maxbd = new BigDecimal(Math.pow(10, precision - scale));
			BigDecimal minbd = maxbd.negate();
			if ((inbd.compareTo(maxbd) >= 0) || (inbd.compareTo(minbd) < 0)) {
				// System.out.println("Max = " + maxbd.toString() + "\nMin = " +
				// minbd + "\nInputted Val: " + inbd.toString());
				// throw new DataTruncation(parameterIndex, true, false,
				// actualLen, expectedLen);
				//throw new SQLException("*** ERROR[29188] Numeric value " + inbd.toPlainString() + " is out of range;" + " Parameter index: " + (parameterIndex) +". ["+new SimpleDateFormat("yyyy-MM-dd HH:mm:s").format(new Date())+"]", "22003", -8411);
				throw new SQLException("*** ERROR[29046] Numeric value out of range, Numeric is " + inbd.toPlainString()  +" ["+new SimpleDateFormat("yyyy-MM-dd HH:mm:s").format(new Date())+"]", "22003", -8411);
			}
		}
	} // end checkDecimalTruncation


	// -------------------------------------------------------------
	/**
	 * This method will check a Decimal value according to the precision in the
	 * Temporary solution...
	 *
	 * @param inbd
	 * @param precision
	 * @param scale
	 *
	 * @return none
	 *
	 */
    static void checkDecimalTruncation(BigDecimal inbd, int precision, int scale)
		    throws SQLException {
	    if (precision <= 0) {
		    return;
	    } else {
		    BigDecimal maxbd = BigDecimal.valueOf(Math.pow(10, precision - scale));
		    BigDecimal minbd = maxbd.negate();
		    if ((inbd.compareTo(maxbd) >= 0) || (inbd.compareTo(minbd) < 0)) {
			    throw new SQLException("*** ERROR[29046] Numeric value out of range, Numeric is " + inbd.toPlainString() + " [" + new SimpleDateFormat("yyyy-MM-dd HH:mm:s").format(new Date()) + "]", "22003", -8411);
		    }
	    }
	    if (scale > 0){
	    	BigDecimal newdb = inbd;
		    if(inbd.scale() > scale){
			    newdb = inbd.setScale(scale,BigDecimal.ROUND_HALF_UP);
		    }
		    newdb =  newdb.movePointRight(scale);
		    BigDecimal maxbd = BigDecimal.valueOf(Math.pow(10, precision));
		    if (newdb.compareTo(maxbd) >= 0){
			    throw new SQLException("*** ERROR[29046] Numeric value out of range, Numeric is " + inbd.toPlainString() + " [" + new SimpleDateFormat("yyyy-MM-dd HH:mm:s").format(new Date()) + "]", "22003", -8411);
		    }
	    }
    } // end checkDecimalTruncation

	// -------------------------------------------------------------
	/**
	 * This method will check a Long value according to the Long.MAX_VALUE and
	 * Long.MIN_VALUE values.
	 * 
	 * @param the
	 *            original long value to check
	 * @param the
	 *            Locale to print the error message in
	 * 
	 * @return none
	 * 
	 */
	static void checkLongBoundary(T4Properties t4Properties, BigDecimal inbd) throws SQLException {
		if ((inbd.compareTo(long_maxbd) > 0) || (inbd.compareTo(long_minbd) < 0)) {
			throw TrafT4Messages.createSQLException(t4Properties, "numeric_out_of_range", inbd.toString());
		}
	} // end checkBigDecimalBoundary

	// -------------------------------------------------------------
	/**
	 * This method will check a Double and long value are the same.
	 * 
	 * @param the
	 *            original double value to check
	 * @param the
	 *            original long value to check
	 * @param the
	 *            Locale to print the error message in
	 * 
	 * @return none
	 * 
	 */
	static void checkLongTruncation(int parameterindex, BigDecimal inbd) throws SQLException {
		long inlong = inbd.longValue();
		double indbl = inbd.doubleValue();

		if ((double) inlong != indbl) {
			int sizeLong = String.valueOf(inlong).length();
			int sizeDbl = String.valueOf(indbl).length();
			// throw new DataTruncation(parameterindex, true, false,
			// sizeLong, sizeDbl);

			DataTruncation dt = new DataTruncation(parameterindex, true, false, sizeLong, sizeDbl);
			dt.setNextException(new SQLException("DataTruncation", "22003", -8411));
			throw dt;
		}
	} // end checkLongTruncation

	/**
	 * This method sets the round mode behaviour for the driver. Accepted values
	 * are: static int ROUND_CEILING Rounding mode to round towards positive
	 * infinity. static int ROUND_DOWN Rounding mode to round towards zero.
	 * static int ROUND_FLOOR Rounding mode to round towards negative infinity.
	 * static int ROUND_HALF_DOWN Rounding mode to round towards "nearest
	 * neighbor" unless both neighbors are equidistant, in which case round
	 * down. static int ROUND_HALF_EVEN Rounding mode to round towards the
	 * "nearest neighbor" unless both neighbors are equidistant, in which case,
	 * round towards the even neighbor. static int ROUND_HALF_UP Rounding mode
	 * to round towards "nearest neighbor" unless both neighbors are
	 * equidistant, in which case round up. static int ROUND_UNNECESSARY
	 * Rounding mode to assert that the requested operation has an exact result,
	 * hence no rounding is necessary. static int ROUND_UP Rounding mode to
	 * round away from zero. The default behaviour is to do ROUND_DOWN.
	 * 
	 * @param ref
	 *            roundMode
	 */
	static int getRoundingMode(String roundMode) {
		int op_roundMode = BigDecimal.ROUND_DOWN;
		if (roundMode == null) {
			op_roundMode = BigDecimal.ROUND_DOWN;
		} else if (roundMode.equals("ROUND_CEILING")) {
			op_roundMode = BigDecimal.ROUND_CEILING;
		} else if (roundMode.equals("ROUND_DOWN")) {
			op_roundMode = BigDecimal.ROUND_DOWN;
		} else if (roundMode.equals("ROUND_FLOOR")) {
			op_roundMode = BigDecimal.ROUND_FLOOR;
		} else if (roundMode.equals("ROUND_HALF_UP")) {
			op_roundMode = BigDecimal.ROUND_HALF_UP;
		} else if (roundMode.equals("ROUND_UNNECESSARY")) {
			op_roundMode = BigDecimal.ROUND_UNNECESSARY;
		} else if (roundMode.equals("ROUND_HALF_EVEN")) {
			op_roundMode = BigDecimal.ROUND_HALF_EVEN;
		} else if (roundMode.equals("ROUND_HALF_DOWN")) {
			op_roundMode = BigDecimal.ROUND_HALF_DOWN;
		} else if (roundMode.equals("ROUND_UP")) {
			op_roundMode = BigDecimal.ROUND_UP;
		} else {
			try {
				op_roundMode = getRoundingMode(Integer.parseInt(roundMode));
			} catch (Exception ex) {
				op_roundMode = BigDecimal.ROUND_DOWN;
			}

		}
		return op_roundMode;
	}

	/**
	 * This method sets the round mode behaviour for the driver. Accepted values
	 * are: static int ROUND_CEILING Rounding mode to round towards positive
	 * infinity. static int ROUND_DOWN Rounding mode to round towards zero.
	 * static int ROUND_FLOOR Rounding mode to round towards negative infinity.
	 * static int ROUND_HALF_DOWN Rounding mode to round towards "nearest
	 * neighbor" unless both neighbors are equidistant, in which case round
	 * down. static int ROUND_HALF_EVEN Rounding mode to round towards the
	 * "nearest neighbor" unless both neighbors are equidistant, in which case,
	 * round towards the even neighbor. static int ROUND_HALF_UP Rounding mode
	 * to round towards "nearest neighbor" unless both neighbors are
	 * equidistant, in which case round up. static int ROUND_UNNECESSARY
	 * Rounding mode to assert that the requested operation has an exact result,
	 * hence no rounding is necessary. static int ROUND_UP Rounding mode to
	 * round away from zero. The default behaviour is to do ROUND_DOWN.
	 * 
	 * @param ref
	 *            roundMode
	 */
	static int getRoundingMode(int roundMode) {
		if ((roundMode == BigDecimal.ROUND_CEILING) || (roundMode == BigDecimal.ROUND_DOWN)
				|| (roundMode == BigDecimal.ROUND_UP) || (roundMode == BigDecimal.ROUND_FLOOR)
				|| (roundMode == BigDecimal.ROUND_HALF_UP) || (roundMode == BigDecimal.ROUND_UNNECESSARY)
				|| (roundMode == BigDecimal.ROUND_HALF_EVEN) || (roundMode == BigDecimal.ROUND_HALF_DOWN)) {
			return roundMode;
		} else {
			return BigDecimal.ROUND_DOWN;
		}
	}

	static BigDecimal setScale(BigDecimal tmpbd, int scale, int roundingMode) throws SQLException {
		try {
			if (scale > -1) {
				tmpbd = tmpbd.setScale(scale, roundingMode);
			}
		} catch (ArithmeticException aex) {
			throw new SQLException(aex.getMessage());
		}
		return tmpbd;
	}

	static final BigDecimal long_maxbd = BigDecimal.valueOf(Long.MAX_VALUE);
	static final BigDecimal long_minbd = BigDecimal.valueOf(Long.MIN_VALUE);

    static private final char DEFAULT_TRIM_WHITESPACE = ' ';

    static public String trimRight(final String string)
    {
        return trimRight(string, DEFAULT_TRIM_WHITESPACE);
    }

    static public String trimRight(final String string, final char trimChar)
    {
        final int lastChar = string.length() - 1;
        int i;

        for (i = lastChar; i >= 0 && string.charAt(i) == trimChar; i--) {
            /* Decrement i until it is equal to the first char that does not
             * match the trimChar given. */
        }
        
        if (i < lastChar) {
            // the +1 is so we include the char at i
            return string.substring(0, i+1);
        } else {
            return string;
        }
    }
    static public String trimLeft(String string)
    {
        return trimLeft( string, DEFAULT_TRIM_WHITESPACE );
    }

    static public String trimLeft(final String string, final char trimChar)
    {
        final int stringLength = string.length();
        int i;
        
        for (i = 0; i < stringLength && string.charAt(i) == trimChar; i++) {
            /* increment i until it is at the location of the first char that
             * does not match the trimChar given. */
        }

        if (i == 0) {
            return string;
        } else {
            return string.substring(i);
        }
    }
    static public String trimRightZeros(String x) {
    	byte[] input = x.getBytes();
    	int i = input.length;
   	
		while (i-- > 0 && input[i] == 0) {}
	
		byte[] output = new byte[i+1];
		System.arraycopy(input, 0, output, 0, i+1);
		return new String(output);
    }
    // wm_merge - AM
    static String convertDateFormat(String dt) {
//        String tokens[] = dt.split("[/]", 3);
//
//        if (tokens.length != 3) {
//            return dt;
//        }
//        StringBuffer sb = new StringBuffer();
//        sb.append(tokens[0]).append("-").append(tokens[1]).append("-").append(tokens[2]);
//        return sb.toString();
        return dt.replaceAll("/", "-");
    }
    /* TODO: this is a horrible hack but what else can be done with random 2 digit/4 digit years for dates?
     * Note: The date constructor wants (year-1900) as a parameter
     * We use the following table for conversion:
     *
     *      Year Value      Assumed Year        Action
     *      <50             Value + 2000        must add 100
     *      >=100           Value               must subtract 1900
     *      >=50            Value + 1900        no change in value needed
     *
     */
    static java.sql.Date valueOf(String s) {
        int year;
        int month;
        int day;
        int firstDash;
        int secondDash;

        if (s == null)
            throw new java.lang.IllegalArgumentException();

        firstDash = s.indexOf('-');
        secondDash = s.indexOf('-', firstDash + 1);
        if ((firstDash > 0) & (secondDash > 0) & (secondDash < s.length() - 1)) {
            year = Integer.parseInt(s.substring(0, firstDash));

            if (year < 50) {//handles 2 digit years: <50 assume 2000, >=50 assume 1900
                year += 100;
            }
            else if(year >= 100) { //handles 4 digit years
                year -= 1900;
            }

            month = Integer.parseInt(s.substring(firstDash + 1, secondDash)) - 1;
            day = Integer.parseInt(s.substring(secondDash + 1));
        } else {
            throw new java.lang.IllegalArgumentException();
        }

        return new java.sql.Date(year, month, day);
    }

} // end class Utility
