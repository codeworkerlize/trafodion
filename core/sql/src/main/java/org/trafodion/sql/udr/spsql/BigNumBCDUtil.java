package org.trafodion.sql.udr.spsql;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.Map;

public class BigNumBCDUtil {
	/**
	 * to convert a BIGNUM  to an ASCII string 
	 * Trailing '\0' is not set!
	 * 
	 * This function first converts the BIGNUM to an BCD representation
	 * then calls convLargeDecToAsciiMxcs converts BCD char to ascii string
	 * @param source
	 * @param sourceLen
	 * @param sourcePrecision
	 * @param sourceScale
	 * @return
	 * @throws Exception
	 */
	public static String convBigNumToAsciiMxcs(byte[] source, int sourceLen, int sourcePrecision, int sourceScale) throws Exception {
		// one extra byte for the sign.
		char[] intermediateLargeDec = new char[sourcePrecision + 1];
		convBigNumToBCD(intermediateLargeDec, sourcePrecision + 1, source, sourceLen);
		String bigNumStr = convLargeDecToAsciiMxcs(intermediateLargeDec, sourcePrecision + 1, sourceScale, 0);
		if(bigNumStr == null) {
			bigNumStr = "";
		}
		bigNumStr = bigNumStr.trim();
		return bigNumStr;
	};

	/**
	 * Convert the Big Num (with sign) into a BCD representation
	 * @param targetData
	 * @param targetLength
	 * @param bigNumByte
	 * @param sourceLen
	 * @throws Exception
	 */
	public static void convBigNumToBCD(char[] targetData, int targetLength, byte[] bigNumByte, int sourceLen) throws Exception {
		byte sign = (byte) (bigNumByte[sourceLen - 1] & 0x80);
		targetData[0] = sign == 0 ? '+' : '-';
		//Temporarily clear source sign
		char[] unsignedTargetData = new char[targetData.length - 1];
		bigNumByte[sourceLen - 1] &= 0x7f;

		Map<Integer, byte[]> bigNumShortMap = new HashMap<>();
		targetLength -= 1;
		int sourceLengthInShorts = sourceLen / 2;
		ByteBuffer bigNumByteBuffer = ByteBuffer.wrap(bigNumByte);
		bigNumByteBuffer.order(ByteOrder.LITTLE_ENDIAN);
		int[] bigNumShort = new int[sourceLengthInShorts];
		int bigNumByteIndex = -1;
		for (int i = 0; i < bigNumShort.length; i++) {
			bigNumShort[i] = getUnsignedByte(bigNumByteBuffer.getShort());
			byte[] shortBytes = new byte[2];
			shortBytes[1] = bigNumByte[++bigNumByteIndex];
			shortBytes[0] = bigNumByte[++bigNumByteIndex];
			bigNumShortMap.put(bigNumShort[i], shortBytes);
		}

		int[] tempSourceDataInShorts = new int[sourceLengthInShorts];
		for (int i = 0; i < tempSourceDataInShorts.length; i++) {
			tempSourceDataInShorts[i] = bigNumShort[i];
		}

		// Ignore trailing zeros in the Big Num. If all zeros, return.
		int actualSourceLengthInShorts = sourceLengthInShorts;
		while (actualSourceLengthInShorts > 0 && tempSourceDataInShorts[actualSourceLengthInShorts - 1] == 0) {
			actualSourceLengthInShorts--;
		}
		if (actualSourceLengthInShorts == 0) {
			return;
		}

		int remainder = 0;
		int finalTargetLength = 0;
		int targetEndIndex = unsignedTargetData.length - 1;
		while ((actualSourceLengthInShorts != 1) || (tempSourceDataInShorts[actualSourceLengthInShorts - 1] >= 10000)) {
			// Divide the Big Num by 10^4. It is more efficient to insert
			// the division code than to call SimpleDivideHelper();
			for (int j = actualSourceLengthInShorts - 1; j >= 0; j--) {
				byte[] bytes2 = bigNumShortMap.get(tempSourceDataInShorts[j]);
				if (bytes2 == null) {
					bytes2 = shortToByte((short) tempSourceDataInShorts[j]);
				}
				byte[] bytes1 = shortToByte((short) remainder);
				byte[] mergeBytes = new byte[bytes1.length + bytes2.length];
				System.arraycopy(bytes1, 0, mergeBytes, 0, bytes1.length);
				System.arraycopy(bytes2, 0, mergeBytes, bytes1.length, bytes2.length);
				ByteBuffer tempByteBuf = ByteBuffer.wrap(mergeBytes);
				int temp = tempByteBuf.getInt();
				tempSourceDataInShorts[j] = temp / 10000;
				remainder = temp % 10000;
			}
			if (tempSourceDataInShorts[actualSourceLengthInShorts - 1] == 0) {
				actualSourceLengthInShorts--;
			}

			// Compute the BCD representation of the remainder and append to the
			// left of the final BCD.
			for (int j = 0; j < 4; j++) {
				if (finalTargetLength >= targetLength) {
					return;
				}
				finalTargetLength++;
				unsignedTargetData[targetEndIndex] = (char) (remainder % 10);
				remainder = (short) (remainder / 10);
				targetEndIndex--;
			}
		}

		// Compute the BCD representation of the final remainder and append to the
		// left of the final BCD.
		remainder = tempSourceDataInShorts[0];
		while (remainder != 0) {
			if (finalTargetLength >= targetLength) {
				return;
			}
			finalTargetLength++;
			unsignedTargetData[targetEndIndex] = (char) (remainder % 10);
			remainder = (short) (remainder / 10);
			targetEndIndex--;
		}

		// append sign
		System.arraycopy(unsignedTargetData, 0, targetData, 1, unsignedTargetData.length);
	}

	/**
	 * converts BCD char to ascii string
	 * @param source
	 * @param srcLen
	 * @param srcScale
	 * @param varCharLenSize
	 * @return
	 * @throws Exception
	 */
	private static String convLargeDecToAsciiMxcs(char[] source, int srcLen, int srcScale, int varCharLenSize)
			throws Exception {
		if (source[0] < 2) {
			int[] realSource = new int[256];
			System.arraycopy(source, 0, realSource, 0, source.length);
			int realLength = 1 + (srcLen - 2) / 5;
			for (int srcPos = srcLen - 1; srcPos > 0; srcPos--) {
				int r = 0;
				for (int i = 1; i <= realLength; i++) {
					int q = (realSource[i] + r) / 10;
					r = (r + realSource[i]) - 10 * q;
					realSource[i] = (short) q;
					r <<= 16;
				}
				source[srcPos] = (char) (r >>= 16);
			}
		}
		// TODO NA_MXCS ?(copy from exp.ExpConvMxcs.cpp convBigNumToAsciiMxcs method)
		int leftPad = 0;
		/*
		 * if(NA_MXCS) { leftPad = 0; } else { leftPad = 1; }
		 */
		if (varCharLenSize > 1) {
			leftPad = 0;
		}

		// skip leading zeros; start with index == 1; index == 0 is sign
		// stop looking one position before the decimal point. This
		// position is sourceLen - sourceScale - 1
		int currPos = 1;
		while ((currPos < (srcLen - srcScale - 1)) && source[currPos] == 0) {
			currPos++;
		}
		int targetLen = srcLen + 1; // add decimal point
		char[] target = new char[targetLen];
		int padLen = target.length;
		if (source[0] == '-') {
			padLen--;
		}
		int requiredDigits = srcLen - currPos - srcScale;
		padLen -= requiredDigits;
		if (padLen < 0) {
			requiredDigits -= padLen;
		}
		if (srcScale > 0) {
			if (padLen > 1) { // fraction exists
				padLen--; // for decimal point
				srcScale = (srcScale < padLen ? srcScale : padLen);
				padLen -= srcScale;
			} else {
				srcScale = 0;// no fraction
			}
		}
		int targetPos = 0;
		// if target is fixed length and leftPad, left pad blanks
		if (leftPad > 0) {
			for (; targetPos < padLen; targetPos++) {
				target[targetPos] = ' ';
			}
		}
		// fill in sign
		if (source[0] == '-') {
			target[targetPos++] = '-';
		}
		// fill in digits
		int i = 0;
		for (i = 0; i < requiredDigits; i++, targetPos++, currPos++) {
			target[targetPos] = (char) (source[currPos] + '0');
		}

		// if we have a scale, add decimal point and some digits
		if (srcScale > 0) {
			target[targetPos++] = '.';
			for (i = 0; i < srcScale; i++, targetPos++, currPos++) {
				target[targetPos] = (char) (source[currPos] + '0');
			}
		}
		;

		// Right pad blanks for fixed char.
		if (leftPad == 0 && varCharLenSize == 0) {
			while (targetPos < target.length) {
				target[targetPos++] = ' ';
			}
		}
		
		// make sure that the source fits in the target
		// we might skip some digits after the decimal point
		// to make it fit
		if (padLen < 0) {
			// target string is not long enough - overflow
			// return EXE_STRING_OVERFLOW;
			throw new Exception("convLargeDecToAsciiMxcs: target string is not long enough - overflow");
		}
		String bigNumStr = new String(target);
		return bigNumStr;
	}

	public static void main(String[] args) {
		byte[] bigNumBytes = { 34, -5, -6, 113, 31, 1, 0, 0, 0, 0 };
		try {
			String bigNumStr = convBigNumToAsciiMxcs(bigNumBytes, bigNumBytes.length, 19, 5);
			System.out.println(bigNumStr);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static byte getUnsignedByte(byte data) {
		return (byte) (data & 0x0FF);
	}

	public static int getUnsignedByte(short data) {
		int result = data & 0x0FFFF;
		return result;
	}

	public static byte[] intToByteArray(int a) {
		return new byte[] { (byte) ((a >> 24) & 0xFF), (byte) ((a >> 16) & 0xFF), (byte) ((a >> 8) & 0xFF),
				(byte) (a & 0xFF) };
	}

	public static byte[] shortToByte(short number) {
		int temp = number;
		byte[] b = new byte[2];
		for (int i = b.length - 1; i > -1; i--) {
			b[i] = new Integer(temp & 0xff).byteValue();// 将最低位保存在最低位
			temp = temp >> 8; // 向右移8位
		}
		return b;
	}
}
