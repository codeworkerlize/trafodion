package org.trafodion.sql.udr.spsql;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import org.trafodion.sql.udr.spsql.Var.Type;

public class ExpDatetime {
	public static final DatetimeFormats FORMAT = DatetimeFormats.DATETIME_FORMAT_DEFAULT;

	/**
	 * This method is used to convert the given datetime value to an ASCII string in
	 * one of three formats (DEFAULT, EUROPEAN, and USA).
	 * 
	 * @param srcData
	 * @param startFieldCode
	 * @param endFieldCode
	 * @param scale
	 * @return
	 * @throws Exception
	 */
	public static String convDatetimeToASCII(byte[] srcData, int startFieldCode, int endFieldCode, int scale)
			throws Exception {
		ByteBuffer dateTimeByteBuf = ByteBuffer.wrap(srcData);
		dateTimeByteBuf.order(ByteOrder.LITTLE_ENDIAN);
		short year = 0;
		char month = 0;
		char day = 0;

		DatetimeField startField = DatetimeField.getInstance(startFieldCode);
		DatetimeField endField = DatetimeField.getInstance(endFieldCode);

		int minReqDstLen = getDatetimeFormatLen(FORMAT, startField, endField, scale);
		// Make sure we have enough room for at least the minimum.
		if ((minReqDstLen <= 0)) {
			// ExRaiseSqlError(heap, diagsArea, EXE_STRING_OVERFLOW);
			throw new Exception("convDatetimeToASCII: error code:8402," + " calcute datetime length fail.");
		}

		// Capture the date portion of the datetime value in the
		// corresponding variable.
		int field;
		DatetimeField dateDay = DatetimeField.DATE_DAY;
		for (field = startField.getCode(); field <= endField.getCode() && field <= dateDay.getCode(); field++) {
			DatetimeField dateFieldItem = DatetimeField.getInstance(field);
			switch (dateFieldItem) {
			case DATE_YEAR:
				year = dateTimeByteBuf.getShort();
				break;
			case DATE_MONTH:
				month = (char) dateTimeByteBuf.get();
				break;
			case DATE_DAY:
				day = (char) dateTimeByteBuf.get();
				break;
			default:
				throw new Exception("convDatetimeToASCII: error code:8402," + " calcute datetime length fail.");
			}
		}

		char[] dstDataPtr = new char[minReqDstLen];
		int dstIndex = 0;
		if (year > 0) {
			convertToAscii(year, dstDataPtr, dstIndex, dstIndex + 4);
			dstIndex += 4;
			if (endField.getCode() > DatetimeField.DATE_YEAR.getCode()) {
				dstDataPtr[dstIndex++] = '-';
			}
		}

		if (month > 0) {
			convertToAscii(month, dstDataPtr, dstIndex, dstIndex + 2);
			dstIndex += 2;
			if (endField.getCode() > DatetimeField.DATE_MONTH.getCode()) {
				dstDataPtr[dstIndex++] = '-';
			}
		}
		if (day > 0) {
			convertToAscii(day, dstDataPtr, dstIndex, dstIndex + 2);
			dstIndex += 2;
		}

		// Add a delimiter between the date and time portion if required.
		if (field > startField.getCode() && field <= endField.getCode()) {
			dstDataPtr[dstIndex++] = ' ';
		}

		// Format the Time portion in the proper format.
		for (; field <= endField.getCode(); field++) {
			DatetimeField dateFieldItem = DatetimeField.getInstance(field);
			switch (dateFieldItem) {
			case DATE_HOUR: {
				char hour = (char) dateTimeByteBuf.get();
				convertToAscii(hour, dstDataPtr, dstIndex, dstIndex+2);
				dstIndex += 2;
				if (endField.getCode() > DatetimeField.DATE_HOUR.getCode()) {
					dstDataPtr[dstIndex++] = ':';
				}
				break;
			}
			case DATE_MINUTE: {
				char minute = (char) dateTimeByteBuf.get();
				convertToAscii(minute, dstDataPtr, dstIndex, dstIndex+2);
				dstIndex += 2;
				if (endField.getCode() > DatetimeField.DATE_MINUTE.getCode()) {
					dstDataPtr[dstIndex++] = ':';
				}
				break;
			}
			case DATE_SECOND: {
				char second = (char) dateTimeByteBuf.get();
				convertToAscii(second, dstDataPtr, dstIndex, dstIndex+2);
				dstIndex += 2;

				// If there is a fraction portion of the second field and there
				// is room in the destination string, format as much of the
				// fraction as possible.
				if (scale > 0) {
					int fraction = dateTimeByteBuf.getInt();
					// If we still have a fraction precision left, format it into
					// the result string.
					dstDataPtr[dstIndex++] = '.';
					convertToAscii(fraction, dstDataPtr, dstIndex, dstIndex + scale);
					dstIndex += scale;
				}
				break;
			}
			default:
				// return -1;
			}
		}
		String result = new String(dstDataPtr);
		return result;
	}

	/**
	 * Format value as a string.
	 * @param fieldValue
	 * @param result
	 * @param off
	 * @param end
	 */
	private static void convertToAscii(int fieldValue, char[] result, int off, int end) {
		while ((fieldValue != 0) && (end > off)) {
			result[--end] = (char) (fieldValue % 10 + '0');
			fieldValue /= 10;
		}
		// Fill in remaining leading characters with '0'
		while (end > off) {
			result[--end] = '0';
		}
	}

	/**
	 * calculate display length
	 * @param format
	 * @param startField
	 * @param endField
	 * @param scale
	 * @return
	 */
	private static int getDatetimeFormatLen(DatetimeFormats format,
			DatetimeField startField, DatetimeField endField, int scale) {
		int minReqDstLen = 0;
		switch (format) {
		case DATETIME_FORMAT_DEFAULT:
		case DATETIME_FORMAT_USA:
		case DATETIME_FORMAT_EUROPEAN: {
			int field;
			for (field = startField.getCode(); field <= endField.getCode(); field++) {
				DatetimeField fieldItem = DatetimeField.getInstance(field);
				switch (fieldItem) {
				case DATE_YEAR:
					minReqDstLen += 5;
					break;
				case DATE_MONTH:
					minReqDstLen += 3;
					break;
				case DATE_DAY:
				case DATE_MINUTE:
					minReqDstLen += 3;
					break;
				case DATE_HOUR:
					minReqDstLen += 3;
					if (format == DatetimeFormats.DATETIME_FORMAT_USA)
						minReqDstLen += 3;
					break;
				case DATE_SECOND:
					minReqDstLen += 3;
					if(scale > 0) {
						minReqDstLen += scale + 1;
					}
					break;
				default:
					return -1;
				}
			}
			// No trailing delimiter required.
			minReqDstLen--;
		}
			break;
		default:
			// return ExpDatetime::getDatetimeFormatLen(format);
		}

		return minReqDstLen;
	}

	enum DatetimeField {
		DATE_UNKNOWN(0), DATE_NOTAPPLICABLE(0), DATE_YEAR(1), DATE_MONTH(2), DATE_DAY(3), DATE_HOUR(4), DATE_MINUTE(
				5), DATE_SECOND(6), DATE_FRACTION_MP(7), // Used in MP only!
		DATE_CENTURY(8), DATE_DECADE(9), DATE_WEEK(10), DATE_QUARTER(11), DATE_EPOCH(12), DATE_DOW(13), DATE_DOY(
				14), DATE_WOM(15), DATE_MAX_SINGLE_FIELD(16),
		// other datetime fields not used in FS2 and DDL
		DATE_YEARQUARTER_EXTRACT(1000), // Used for EXTRACT (DATE_PART) function only!
		DATE_YEARMONTH_EXTRACT(1001), // Used for EXTRACT (DATE_PART) function only!
		DATE_YEARWEEK_EXTRACT(1002), // Used for EXTRACT (DATE_PART) function only!
		DATE_YEARQUARTER_D_EXTRACT(1003), // Used for EXTRACT (DATE_PART) function only!
		DATE_YEARMONTH_D_EXTRACT(1004), // Used for EXTRACT (DATE_PART) function only!
		DATE_YEARWEEK_D_EXTRACT(1005) // Used for EXTRACT (DATE_PART) function only!
		;

		private static final Map<Integer, DatetimeField> INSTANCE_MAP = new HashMap<>();

		int code;

		static {
			for (DatetimeField item : DatetimeField.values()) {
				INSTANCE_MAP.put(item.code, item);
			}
		}

		private DatetimeField(int code) {
			this.code = code;
		}

		public int getCode() {
			return code;
		}

		public static DatetimeField getInstance(int code) {
			return INSTANCE_MAP.get(code);
		}
	};

        public static Object strToDateTime(String datetimeStr, Type colType, int scale) throws ParseException {
          Object dateTimeVal = null;
          SimpleDateFormat format = null;
          StringBuilder formatStr = new StringBuilder();
          switch (colType) {
          case DATE:
	    dateTimeVal = java.sql.Date.valueOf(datetimeStr);
            break;
          case TIME:
	    dateTimeVal = Time.valueOf(datetimeStr);
            break;
          default:
            dateTimeVal = Timestamp.valueOf(datetimeStr);
            break;
          }

          return dateTimeVal;
        }

	public static void main(String[] args) {
		byte[] dateBytes = {-29, 7, 3, 26, 12, 10, 12, 58, -30, 1, 0};
		try {
			convDatetimeToASCII(dateBytes, 1, 6, 6);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
