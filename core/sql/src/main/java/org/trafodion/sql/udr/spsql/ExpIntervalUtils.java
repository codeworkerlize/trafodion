package org.trafodion.sql.udr.spsql;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;

public class ExpIntervalUtils {

	static final int SQL_SMALL_SIZE = 2;
	static final int SQL_INT_SIZE = 4;
	static final int SQL_LARGE_SIZE = 8;

	static final char[] DELIMITER = { '^', '-', '^', ' ', ':', ':' };
	static final int[] FIELD_VALUE_MAX = { 0, 11, 0, 23, 59, 59 };

	/**
	 * This method is used to convert the given interval value to an ASCII string
	 * 
	 * @param srcData
	 * @param size
	 * @param startFieldCode
	 * @param endFieldCode
	 * @param scale
	 * @return
	 * @throws Exception
	 */
	public static String convIntervalToASCII(byte[] srcData, int size, int startFieldCode, int endFieldCode,
			int leadingPrecision, int fractionPrecision) throws Exception {
		ByteBuffer byteBuf = ByteBuffer.wrap(srcData);
		byteBuf.order(ByteOrder.LITTLE_ENDIAN);

		long value;
		switch (size) {
		case SQL_SMALL_SIZE:
			value = byteBuf.getShort();
			break;
		case SQL_INT_SIZE:
			value = byteBuf.getInt();
			break;
		case SQL_LARGE_SIZE:
			value = byteBuf.getLong();
			break;
		default:
			throw new Exception("interval size is incorrect.");
		}
		char sign = '+';
		if (value < 0) {
			sign = '-';
			value = -value;
		}

		// 1 for -ve sign, each field is 2 + delimiter
		int realTargetLen = (sign == '-' ? 1 : 0) + leadingPrecision + (endFieldCode - startFieldCode) * 3;
		int curTargetIndex = realTargetLen;
		if (fractionPrecision > 0) {
			realTargetLen += fractionPrecision + 1; // 1 for '.'
		}

		char target[] = new char[realTargetLen];

		// left blankpad the target.
		if (startFieldCode == IntervalField.DATE_YEAR.getCode() || startFieldCode == IntervalField.DATE_DAY.getCode()
				|| endFieldCode == IntervalField.DATE_YEAR.getCode()
				|| endFieldCode == IntervalField.DATE_DAY.getCode()) {
			for (int i = 0; i < leadingPrecision; i++) {
				target[i] = ' ';
			}

		}

		long factor = 1;
		long fieldVal = -1;
		if (fractionPrecision > 0) {
			for (int i = fractionPrecision; i > 0; i--) {
				factor *= 10;
			}
			fieldVal = value;
			value = value / factor;
			fieldVal -= value * factor;
		}

		// convert fraction part if any. targetIndex was set earlier
		if (fractionPrecision > 0) {
			curTargetIndex += 1; // 1 for '.'
			for (int i = curTargetIndex; i < target.length; i++) {
				target[i] = '0';
			}
			convInt64ToAsciiMxcs(target, curTargetIndex, fractionPrecision, // targetLen
					fieldVal, 0, // scale,
					'0', // filler character
					0, 1); // leftPad
			curTargetIndex--;
			target[curTargetIndex] = '.';
		};

		// Now rest of the fields except leading field...
		for (int field = endFieldCode; field > startFieldCode; field--) {
			int index = field - IntervalField.DATE_YEAR.getCode(); // zero-based array index!

			factor = FIELD_VALUE_MAX[index] + 1;
			fieldVal = value;
			value = value / factor;
			fieldVal -= value * factor;
			curTargetIndex -= 2;

			convInt64ToAsciiMxcs(target, curTargetIndex, 2, // targetLen
					(long) fieldVal, 0, // scale,
					'0', // filler character
					0, 1); // leftPad

			curTargetIndex--;
			target[curTargetIndex] = DELIMITER[index];
		};

		// leading field
		curTargetIndex = sign == '-' ? 1 : 0;
		convInt64ToAsciiMxcs(target, curTargetIndex, leadingPrecision, // targetLen
				value, 0, // scale,
				' ', // filler character
				0, 1); // leftPad
		String result = new String(target);
		result = result.trim();

		if (sign == '-') {
			result = sign + result;
		}

		return result;
	}

	static void convInt64ToAsciiMxcs(char[] target, int targetIndex, int targetLen, long source, int scale, char filler,
			int leadingSign, int leftPad) throws Exception {
		long digitCnt = 0;
		int negative = source < 0 ? 1 : 0;
		short fixRightMost = 0; // true if need to fix the rightmost digit.

		long padLen = targetLen;
		long requiredDigits = 0;
		long leftMost; // leftmost digit.
		int rightMost; // rightmost digit.
		long sign = 0;

		long newSource = 0;
		// 0x8000000000000000)) // = -2 ** 63
		if ((negative > 0) && (source == Math.pow(-2, 63))){
			// newSource = 0x7fffffffffffffff;
			// 123456789012345
			newSource = 123456789012345l;
			digitCnt = 19;
			fixRightMost = 1;
		} else {
			newSource = (negative > 0 ? -source : source);
			digitCnt = (newSource + "").length();
		}
		if (leadingSign > 0 || negative > 0) {
			sign = 1;
			padLen--;
		}

		// No truncation allowed.
		requiredDigits = digitCnt;
		// Add extra zero's.
		if (scale > requiredDigits) {
			requiredDigits += (scale - requiredDigits);
		}
		padLen -= requiredDigits;
		if (scale > 0) {
			padLen--; // decimal point
		}
		if (padLen < 0) {
			// target string is not long enough - overflow
			// return EXE_STRING_OVERFLOW;
			throw new Exception("error code: -8402, interval size error");
		}

		if (leftPad > 0) {
			leftMost = padLen + sign;
		} else {
			leftMost = sign;
		}

		int currPos;
		// Add filler.
		rightMost = currPos = targetLen - 1;
		if (padLen > 0) {
			long start;
			// Pad to the left.
			if (leftPad > 0) {
				start = sign;
			} else { // Pad to the right
				start = targetLen - padLen;
				rightMost = currPos = (int) (start - 1);
			}
                        for(int i = (int)start; i < (int)padLen; i++) {
                            target[i + targetIndex] = filler;
                        }
		}

		// Convert the fraction part and add decimal point.
		targetIndex += currPos;
		if (scale > 0) {
			long low = (currPos - scale);
			for (; currPos > low; currPos--) {
				target[targetIndex--] = (char) ((newSource % 10) + '0');
				newSource /= 10;
			}
			target[targetIndex--] = '.';
		}

		// Convert the integer part.
		for (; currPos >= leftMost; currPos--) {
			target[targetIndex--] = (char) ((newSource % 10) + '0');
			newSource /= 10;
		}

		// Add sign.
		if (leadingSign > 0) {
			if (negative > 0) {
				target[0] = '-';
			} else {
				target[0] = '+';
			}
		} else if (negative > 0) {
			target[0] = '-';
		}

		// Fix the rightmost digit for -2 ** 63.
		// TODO when go to here
		if (fixRightMost > 0 && target[rightMost] == '7') {
			target[rightMost] = '8';
		}

		if (newSource != 0 || currPos < -1) { // Sanity check fails.
			// return EXE_STRING_OVERFLOW;
			throw new Exception("error code: -8402, interval size error");
		}
	}

	enum IntervalField {
		DATE_UNKNOWN(0), DATE_YEAR(1), DATE_MONTH(2), DATE_DAY(3), DATE_HOUR(4), DATE_MINUTE(5), DATE_SECOND(6);

		private static final Map<Integer, IntervalField> INSTANCE_MAP = new HashMap<>();

		int code;

		static {
			for (IntervalField item : IntervalField.values()) {
				INSTANCE_MAP.put(item.code, item);
			}
		}

		private IntervalField(int code) {
			this.code = code;
		}

		public int getCode() {
			return code;
		}

		public static IntervalField getInstance(int code) {
			return INSTANCE_MAP.get(code);
		}
	};

  public static String genarateFullIntervalExp(String colStr, Column column) {
    int valueType = column.getFsDataType();

    String fullInterval = "";
    switch (valueType) {
    case 195:
      fullInterval = "INTERVAL '" + colStr + "' YEAR (" + column.getPrecision() + ")";
      break;
    case 196:
      fullInterval = "INTERVAL '" + colStr + "' MONTH (" + column.getPrecision() + ")";
      break;
    case 198:
      fullInterval = "INTERVAL '" + colStr + "' DAY (" + column.getPrecision() + ")";
      break;
    case 199:
      fullInterval = "INTERVAL '" + colStr + "' HOUR (" + column.getPrecision() + ")";
      break;
    case 201:
      fullInterval = "INTERVAL '" + colStr + "' MINUTE (" + column.getPrecision() + ")";
      break;
    case 204:
      fullInterval = "INTERVAL '" + colStr + "' SECOND (" + column.getPrecision() + "," + column.getScale() +")";
      break;
    case 197:
      fullInterval = "INTERVAL '" + colStr + "' YEAR (" + column.getPrecision() + ") TO MONTH";
      break;
    case 200:
      fullInterval = "INTERVAL '" + colStr + "' DAY (" + column.getPrecision() + ") TO HOUR";
      break;
    case 203:
      fullInterval = "INTERVAL '" + colStr + "' DAY (" + column.getPrecision() + ") TO MINUTE";
      break;
    case 207:
      fullInterval = "INTERVAL '" + colStr + "' DAY (" + column.getPrecision() + ") TO SECOND (" + column.getScale() + ")";
      break;
    case 202:
      fullInterval = "INTERVAL '" + colStr + "' HOUR (" + column.getPrecision() + ") TO MINUTE";
      break;
    case 206:
      fullInterval = "INTERVAL '" + colStr + "' HOUR (" + column.getPrecision() + ") TO SECOND (" + column.getScale() + ")";
      break;
    case 205:
      fullInterval = "INTERVAL '" + colStr + "' MINUTE (" + column.getPrecision() + ") TO SECOND (" + column.getScale() + ")";
      break;
    default:
      fullInterval = colStr;
    }

    return fullInterval;
  }
  
	public static void main(String[] args) {
		byte[] colValueBytes = { -1, -65, -68, -9, -23, 10, 0, 0 };
		try {
			convIntervalToASCII(colValueBytes, 8, 1, 2, 17, 0);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
