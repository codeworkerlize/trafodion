package org.trafodion.sql.udr.spsql;

import java.math.BigDecimal;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.List;
import java.nio.ByteBuffer;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.trafodion.sql.udr.spsql.Var.Type;
import org.trafodion.sql.udr.spsql.functions.*;

public class ProcessColumn {
    public static final int LONG_SIZE = 8;
    public static final int DOUBLE_SIZE = 8;
    public static final int FLOAT_SIZE = 8;
    public static final int INT_SIZE = 4;
    public static final int SHORT_SIZE = 2;
    public static final int BYTE_SIZE = 1;
    private static final Logger LOG = Logger.getLogger(ProcessColumn.class);

    private int fixedOffset;
    private boolean dataExists;
    private int varcharPos;
    private int curVarCount;
    private int varCount;
    private int[] varArr;

    public ProcessColumn(int fixedOffset, boolean dataExists, int varcharPos, int curVarCount, int varCount, int[] varArr) throws Exception {
        this.fixedOffset = fixedOffset;
        this.dataExists = dataExists;
        this.varcharPos = varcharPos;
        this.curVarCount = curVarCount;
        this.varCount = varCount;
        this.varArr = varArr;
    }

    public void setFixedOffset(int fixedOffset) {
        this.fixedOffset = fixedOffset;
    }
    public int getFixedOffset() {
        return fixedOffset;
    }
    public void setDataExists(boolean dataExists) {
        this.dataExists = dataExists;
    }
    public boolean getDataExists() {
        return dataExists;
    }
    public void setVarcharPos(int varcharPos) {
        this.varcharPos = varcharPos;
    }
    public int getVarcharPos() {
        return varcharPos;
    }
    public void setCurVarCount(int curVarCount) {
        this.curVarCount = curVarCount;
    }
    public int getCurVarCount() {
        return curVarCount;
    }
    public int getvarCount() {
        return varCount;
    }
    public int[] getVarArr() {
        return varArr;
    }
    
    public void dealNullValue(Column col) {
        Var nullVar = new Var();
        nullVar.setName(col.getName());
        col.setValue(nullVar);
    }

    private String getDefaultValue(Column col, Type colType) {
        String defValue = null;
        if (col.getDefaultClass() == 3) {
            defValue = String.valueOf(col.getDefaultValue());
        }
        return defValue;
    }

    private int getFixedOffset(boolean isAddCol, int fixedOffset, int dataAligement) {
        if (dataAligement == 0) {
            return fixedOffset;
        }

        int size = 0;      
        if (isAddCol && dataAligement == 8) {
            size = 4;
        } else {
            size = dataAligement;
        }

        if (fixedOffset > 0) {
            return ((((fixedOffset - 1)/size) + 1) * dataAligement);
        } else {
            return fixedOffset;
        }
    }

    public void FixedAligementOffset(Column col, ByteBuffer rowBuf, boolean isOldValue) {
        if (col == null) {
            return;
        }

        String colTypeStr = col.getType();
        int needFixedOffsetNeed = getFixedOffset(col.getAddCol(), getFixedOffset(), col.getHbaseDataAligement());
        if (needFixedOffsetNeed > getFixedOffset()) {
            rowBuf.position(needFixedOffsetNeed);
            setFixedOffset(needFixedOffsetNeed);
        }
        if (isOldValue) {
            if ((!colTypeStr.equals("VARCHAR")) && getVarcharPos() > 0) {
                if (getVarcharPos() - getFixedOffset() < 1) {
                    setDataExists(false);
                }
            } else if (rowBuf.remaining() == 0) {
                setDataExists(false);
            } else {
                setDataExists(true);
            }
            LOG.trace("process old data, is addcolums: " + col.getAddCol() + " ,fixedoffset is: " + getFixedOffset() + " ,VarcharPos is " +
                      getVarcharPos() + " ,ramaing is " + rowBuf.remaining());
        }
    }

    private Object getSmallIntColData(ByteBuffer row, String colType, int scale, String defaultValue) throws UnsupportedEncodingException {
        Object colVal = null;

        short smallintVal = 0;
        if (row == null && defaultValue != null) {
            smallintVal = Short.valueOf(defaultValue);
        } else {
            smallintVal = row.getShort();
        }
        if("UNSIGNED SMALLINT".equals(colType)) {
            colVal = Long.valueOf(smallintVal & 0x0FFFF);
        } else {
            colVal = Long.valueOf(smallintVal);
        }
        if(scale > 0) {
            StringBuffer colValStr = new StringBuffer(colVal + "");
            colValStr.insert(colValStr.length() - scale, ".");
            colVal = new BigDecimal(colValStr.toString());
        }
        return colVal;
    }
    
    public void processSmallint(Column col, ByteBuffer rowBuf) throws Exception{
        if (col == null) {
            return;
        }
        
        int colScale = col.getScale();
        Type colType = null;
        String colTypeStr = col.getType();
        String colName = col.getName();
        Object colVal = null;
        boolean setNullValue = false;
        if (colScale > 0) {
            colType = Type.DECIMAL;
        } else {
            colType = Type.BIGINT;
        }
        if (col.isNull()) {
            dealNullValue(col);
            rowBuf.getShort();
            setFixedOffset(getFixedOffset() + SHORT_SIZE);
        } else {
            if (getDataExists()) {
                colVal = getSmallIntColData(rowBuf, colTypeStr, colScale, null);
                setFixedOffset(getFixedOffset() + SHORT_SIZE);
            } else if (col.getDefaultClass() == 3) {
                colVal = getSmallIntColData(null, colTypeStr, colScale, col.getDefaultValue());
            } else {
                setNullValue = true;
            }
            LOG.trace("colName: " + colName + " ,Column value: " + colVal);
            if (!setNullValue) {
                col.setValue(new Var(colName, colType, colVal));
            } else {
                dealNullValue(col);
            }
        } 
    }

    public void processInterval(Column col, ByteBuffer rowBuf) throws Exception{
        if (col == null) {
            return;
        }

        int colSize = col.getLen();
        String colStr = null;
        boolean setNullValue = false;
        String colName = col.getName();
        
        if (col.isNull()) {
            dealNullValue(col);
            byte[] colValBytes = new byte[colSize];
            rowBuf.get(colValBytes);
            setFixedOffset(getFixedOffset() + colSize);
        } else {
            if (getDataExists()) {
                byte[] colValBytes = new byte[colSize];
                rowBuf.get(colValBytes);
                colStr = ExpIntervalUtils.convIntervalToASCII(colValBytes,
                                                              col.getLen(), col.getDateTimeStartField(),
                                                              col.getDateTimeEndField(), col.getPrecision(),
                                                              col.getScale());
                setFixedOffset(getFixedOffset() + colSize);
            } else if (col.getDefaultClass() == 3) {
                colStr = col.getDefaultValue();
            } else {
                setNullValue = true;
            }
            
            if (!setNullValue) {
                colStr = ExpIntervalUtils.genarateFullIntervalExp(colStr, col);
                LOG.trace("colName: " + colName + " ,Column value: " + colStr);
                Var colVar = new Var(colName, Type.INTERVAL, colStr);
                colVar.setScale(col.getScale());
                col.setValue(colVar);
            } else {
                dealNullValue(col);
            }
                
        }
    }

    private Object getTinyIntColData(ByteBuffer row, String colType, int scale, String defaultValue) throws UnsupportedEncodingException {
        Object colVal = null;

        if (row == null && defaultValue != null) {
            colVal = Long.valueOf(defaultValue);
        } else {
            byte byteVal = row.get();
            if("UNSIGNED TINYINT".equals(colType)) {
                colVal = Long.valueOf(byteVal & 0xff);
            } else {
            colVal = Long.valueOf(byteVal);
            }
        }

        if(scale > 0) {
            StringBuffer colValStr = new StringBuffer(colVal + "");
            colValStr.insert(colValStr.length() - scale, ".");
            colVal = new BigDecimal(colValStr.toString());
        }
        return colVal;
    }
    
    public void processTinyint(Column col, ByteBuffer rowBuf) throws Exception{
        if (col == null) {
            return;
        }

        Type colType = null;
        int colScale = col.getScale();
        boolean setNullValue = false;
        String colName = col.getName();
        Object colVal = null;
        
        if (col.isNull()) {
            dealNullValue(col);
            rowBuf.get();
            setFixedOffset(getFixedOffset() + BYTE_SIZE);
        } else {
            if(colScale > 0) {
                colType = Type.DECIMAL;
            } else {
                colType = Type.BIGINT;
            }
            if (getDataExists()) {
                colVal = getTinyIntColData(rowBuf, col.getType(), colScale, null);
                setFixedOffset(getFixedOffset() + BYTE_SIZE);
            } else if (col.getDefaultClass() == 3) {
                colVal = getTinyIntColData(null, col.getType(), colScale, col.getDefaultValue());
            } else {
                setNullValue = true;
            }
            
            if (!setNullValue) {
                LOG.trace("colName: " + colName + " ,Column value: " + colVal);
                col.setValue(new Var(colName, colType, colVal));
            } else {
                dealNullValue(col);
            }
        }
    }

    public void processReal(Column col, ByteBuffer rowBuf) {
        if (col == null) {
            return;
        }

        boolean setNullValue = false;
        String colName = col.getName();
        
        if (col.isNull()) {
            dealNullValue(col);
            rowBuf.getFloat();
            setFixedOffset(getFixedOffset() + FLOAT_SIZE);
        } else {
            double doubleColVal = 0;
            if (getDataExists()) {
                doubleColVal = rowBuf.getFloat();
                setFixedOffset(getFixedOffset() + FLOAT_SIZE);
            } else if (col.getDefaultClass() == 3) {
                doubleColVal = Double.valueOf(col.getDefaultValue());
            } else {
                setNullValue = true;
            }
            
            if (!setNullValue) {
                LOG.trace("colName: " + colName + " ,Column value: " + doubleColVal);
                Var doubleVal = new Var(doubleColVal);
                doubleVal.setName(colName);
                col.setValue(doubleVal);
            } else {
                dealNullValue(col);
            }
        }
    }    

    private Object getIntColData(ByteBuffer row, String colType, int scale, String defaultValue) throws UnsupportedEncodingException {
        Object colVal = null;
        int intVal = 0;
        if (row == null && defaultValue != null) {
            intVal = Integer.valueOf(defaultValue);
        } else {
            intVal = row.getInt();
        }
        //unsigned int need to convert to long
        if("UNSIGNED INTEGER".equals(colType)) {
            colVal = intVal & 0x0FFFFFFFFl;
        } else {
            colVal = Long.valueOf(intVal);
        }
        if(scale > 0) {
            StringBuffer colValStr = new StringBuffer(colVal + "");
            colValStr.insert(colValStr.length() - scale, ".");
            colVal = new BigDecimal(colValStr.toString());
        }
        return colVal;
    }
    
    public void processInterger(Column col, ByteBuffer rowBuf) throws Exception {
        if (col == null) {
            return;
        }

        boolean setNullValue = false;
        String colName = col.getName();
        Type colType = null;
        
        if (col.isNull()) {
            dealNullValue(col);
            rowBuf.getInt();
            setFixedOffset(getFixedOffset() + INT_SIZE);
        } else {
            if (col.getScale() > 0) {
                colType = Type.DECIMAL;
            } else {
                colType = Type.BIGINT;
            }
            Object colVal = null;
            if (getDataExists()) {
                colVal = getIntColData(rowBuf, col.getType(), col.getScale(), null);
                setFixedOffset(getFixedOffset() + INT_SIZE);
            } else if (col.getDefaultClass() == 3) {
                colVal = getIntColData(null, col.getType(), col.getScale(), col.getDefaultValue());
            } else {
                setNullValue = true;
            }
            
            if (!setNullValue) {
                LOG.trace("colName: " + colName + " ,Column value: " + colVal);
                col.setValue(new Var(colName, colType, colVal));
            } else {
                dealNullValue(col);
            }
        }
    }    

    public void processDouble(Column col, ByteBuffer rowBuf) {
        if (col == null) {
            return;
        }

        boolean setNullValue = false;
        String colName = col.getName();
            
        if (col.isNull()) {
            dealNullValue(col);
            rowBuf.getDouble();
            setFixedOffset(getFixedOffset() + DOUBLE_SIZE);
        } else {
            double doubleColVal = 0;
            if (getDataExists()) {
                doubleColVal = rowBuf.getDouble();
                setFixedOffset(getFixedOffset() + DOUBLE_SIZE);
            } else if (col.getDefaultClass() == 3) {
                doubleColVal = Double.valueOf(col.getDefaultValue());
            } else {
                setNullValue = true;
            }
            
            if (!setNullValue) {
                LOG.trace("colName: " + colName + " ,Column value: " + doubleColVal);
                Var doubleVal = new Var(doubleColVal);
                doubleVal.setName(colName);
                col.setValue(doubleVal);
            } else {
                dealNullValue(col);
            }
        }
    }    

    public void processDatetime(Column col, ByteBuffer rowBuf) throws Exception{
        if (col == null) {
            return;
        }

        boolean setNullValue = false;
        String colName = col.getName();
        int colSize = col.getLen();
        Type colType = null;

        if (col.isNull()) {
            dealNullValue(col);
            rowBuf.get(new byte[colSize]);
            setFixedOffset(getFixedOffset() + colSize);
        } else {
            if(colSize == 4 && col.getScale() == 0) {
                colType = Type.DATE;
            } else if(colSize == 3 ||
                      (colSize == 7 && col.getScale() > 0)) {
                colType = Type.TIME;
            } else {
                colType = Type.TIMESTAMP;
            }

            String datetimeStr = null;
            if (getDataExists()) {
                byte[] colValBytes = new byte[colSize];
                rowBuf.get(colValBytes);
                datetimeStr = ExpDatetime.convDatetimeToASCII(colValBytes,
                                                              col.getDateTimeStartField(), col.getDateTimeEndField(),
                                                              col.getScale());
                setFixedOffset(getFixedOffset() + colSize);
            } else if (col.getDefaultClass() == 3) {
                datetimeStr = col.getDefaultValue();
            } else {
                setNullValue = true;
            }
            
            if (!setNullValue) {
                LOG.trace("colName: " + colName + " ,Column value: " + datetimeStr);
                Object dateVal = ExpDatetime.strToDateTime(datetimeStr, colType, col.getScale());
                Var datetimeVal = new Var(colName, colType, dateVal);
                datetimeVal.setLen(col.getLen());
                datetimeVal.setScale(col.getScale());
                col.setValue(datetimeVal);
            } else {
                dealNullValue(col);
            }
        }
    }    

    public void processDecimal(Column col, ByteBuffer rowBuf) {
        if (col == null) {
            return;
        }

        boolean setNullValue = false;
        String colName = col.getName();
        int colSize = col.getLen();

        if (col.isNull()) {
            dealNullValue(col);
            rowBuf.get(new byte[colSize]);
            setFixedOffset(getFixedOffset() + colSize);
        } else {
            String numStr = null;
            if (getDataExists()) {
                byte[] numByte = new byte[colSize];
                rowBuf.get(numByte);

                boolean negative = false;
                if(col.getType().startsWith("SIGNED")) {
                    if ((numByte[0] & 0x80) > 0) {
                        negative = true;
                    }
                    numByte[0] &= 0x7F;
                }
                numStr =  new String(numByte);
                if (Long.parseLong(numStr) == 0) {
                    numStr = "0";
                }

                if (!numStr.equals("0")) {
                    numStr = numStr.replaceFirst("^0*", "");
                    if (col.getScale() > 0) {
                        StringBuffer numStrBuf = new StringBuffer(numStr);
                        numStrBuf.insert(numStr.length() - col.getScale(), ".");
                        numStr = numStrBuf.toString();
                    }
                }
                if (negative) {
                    numStr = "-" + numStr;
                }
                
                setFixedOffset(getFixedOffset() + colSize);
            } else if (col.getDefaultClass() == 3) {
                numStr = col.getDefaultValue();
            } else {
                setNullValue = true;
            }
            
            if (!setNullValue) {
                LOG.trace("colName: " + colName + " ,Column value: " + numStr);
                Var colVal = new Var(colName, Type.DECIMAL, new BigDecimal(numStr));
                col.setValue(colVal);
            } else {
                dealNullValue(col);
            }
        }
    }    

    public void processNumeric(Column col, ByteBuffer rowBuf) throws Exception{
        if (col == null) {
            return;
        }

        boolean setNullValue = false;
        String colName = col.getName();
        int colSize = col.getLen();
        
        if (col.isNull()) {
            dealNullValue(col);
            rowBuf.get(new byte[colSize]);
            setFixedOffset(getFixedOffset() + colSize);
        } else {
            String numStr = null;
            if (getDataExists()) {
                byte[] numByte = new byte[colSize];
                rowBuf.get(numByte);
                numStr = BigNumBCDUtil.convBigNumToAsciiMxcs(numByte, col.getLen(),
                                                             col.getPrecision(), col.getScale());
                
                setFixedOffset(getFixedOffset() + colSize);
            } else if (col.getDefaultClass() == 3) {
                numStr = col.getDefaultValue();
            } else {
                setNullValue = true;
            }
            
            if (!setNullValue) {
                LOG.trace("colName: " + colName + " ,Column value: " + numStr);
                Var colVal = new Var(colName, Type.DECIMAL, new BigDecimal(numStr));
                col.setValue(colVal);
            } else {
                dealNullValue(col);
            }
        }
    }    

    public void processLargeint(Column col, ByteBuffer rowBuf) {
        if (col == null) {
            return;
        }

        boolean setNullValue = false;
        String colName = col.getName();
        int colSize = col.getLen();
        Type colType = null;
        
        if (col.isNull()) {
            dealNullValue(col);
            rowBuf.getLong();
            setFixedOffset(getFixedOffset() + LONG_SIZE);
        } else {
            if(col.getScale() > 0) {
                colType = Type.DOUBLE;
            } else {
                colType = Type.BIGINT;
            }
            
            Object colVal = null;
            if (getDataExists()) {
                colVal = rowBuf.getLong();
                if (col.getScale() > 0) {
                    StringBuffer colValStr = new StringBuffer(colVal+"");
                    if (colValStr.length() > 1) {
                        colValStr.insert(colValStr.length() - col.getScale(), ".");
                        colVal = Double.valueOf(colValStr.toString());
                    }
                }
                setFixedOffset(getFixedOffset() + LONG_SIZE);
            } else if (col.getDefaultClass() == 3) {
                colVal = Double.valueOf(col.getDefaultValue());
            } else {
                setNullValue = true;
            }
            
            if (!setNullValue) {
                LOG.trace("colName: " + colName + " ,Column value: " + colVal);
                col.setValue(new Var(colName, colType, colVal));
            } else {
                dealNullValue(col);
            }
        }
    }    

    public void processCharacter(Column col, ByteBuffer rowBuf) throws Exception{
        if (col == null) {
            return;
        }

        boolean setNullValue = false;
        String colName = col.getName();
        int colSize = col.getLen();

        LOG.trace("colName: " + colName + " ,Column is NULL: " + col.isNull());
        LOG.trace("colName: " + colName + " ,Column existsData: " + getDataExists());
        if (col.isNull()) {
            dealNullValue(col);
            rowBuf.get(new byte[colSize]);
            setFixedOffset(getFixedOffset() + colSize);
        } else {
            String charStr = "";
            if (getDataExists()) {
                byte[] charColVal = new byte[colSize];
                rowBuf.get(charColVal);
                setFixedOffset(getFixedOffset() + colSize);
                LOG.trace("charColVal is " + Arrays.toString(charColVal));
                if (charColVal[0] == 0) {
                    dealNullValue(col);
                    LOG.trace("colName: " + colName + " ,Column value is NULL");
                    return;
                }
                if(Column.CHARACTER_SET_UCS2.equals(col.getCharacterSet())) {
                    charStr = new String(charColVal, "UTF-16LE");
                    LOG.trace("not utf8 colName: " + colName + " ,Column value: " + charStr);
                } else {
                    charStr = new String(charColVal, "UTF-8");
                    LOG.trace("utf8 colName: " + colName + " ,Column value: " + charStr);
                    if (col.getPrecision() > 0) {
                        charStr = charStr.substring(0, col.getPrecision());
                    }
                }

            } else if (col.getDefaultClass() == 3) {
                charStr = col.getDefaultValue();
            } else {
                setNullValue = true;
            }
            
            if (!setNullValue) {
                LOG.trace("colName: " + colName + " ,Column value: " + charStr);
                Var charVar = new Var(charStr);
                charVar.setName(colName);
                col.setValue(charVar);
            } else {
                dealNullValue(col);
            }
        }
    }    

  /**
   * 
   * @param oldRow
   * @param varArr
   * @param varIndex
   * @return
   * @throws UnsupportedEncodingException 
   */
    private String getValColData(ByteBuffer row, int[] varArr, int varIndex, String characterSet) throws UnsupportedEncodingException {
        int dataStartIndex = varArr[varIndex];
        int dataLength = row.getInt(dataStartIndex);
        dataStartIndex += 4;
        byte[] byteData = new byte[dataLength];
        for(int i = 0; i < dataLength; i++) {
            byteData[i] = row.get(dataStartIndex);
            dataStartIndex++;
        }
        String colValue = "";
        if(Column.CHARACTER_SET_UCS2.equalsIgnoreCase(characterSet)) {
            colValue = new String(byteData, "UTF-16LE");
        } else {
            colValue = new String(byteData, "UTF-8");
        }
        LOG.trace("var column:" + varIndex + ", column value:" + colValue);
        return colValue;
    }
    
    public void processVarchar(Column col, ByteBuffer rowBuf) throws Exception{
        if (col == null) {
            return;
        }

        String colName = col.getName();

        LOG.trace("colName: " + colName + " ,varCount is: " + getvarCount() + " ,CurVarCount is: " + getCurVarCount());
        if ((getvarCount() - getCurVarCount() < 1)) {
            if (col.getDefaultClass() == 2) {
                dealNullValue(col);
            } else {
                String varColVal = col.getDefaultValue();
                LOG.trace("colName: " + colName + " ,Column value:" + varColVal);
                Var charVar = new Var(varColVal);
                charVar.setName(colName);
                col.setValue(charVar);
                setCurVarCount(getCurVarCount() + 1);
            }
            return;
        }
        
        if (col.isNull()) {
            dealNullValue(col);
            getValColData(rowBuf, getVarArr(), getCurVarCount(), col.getCharacterSet());
        } else {
            String varColVal = getValColData(rowBuf, getVarArr(), getCurVarCount(), col.getCharacterSet());
            LOG.trace("colName: " + colName + " ,Column value:" + varColVal);
            Var charVar = new Var(varColVal);
            charVar.setName(colName);
            col.setValue(charVar);
        }
        setCurVarCount(getCurVarCount() + 1);
    }    

    public void processLOB(Column col, ByteBuffer rowBuf) {
        if (col == null) {
            return;
        }
        String colName = col.getName();
        LOG.trace(" not process lob object");
        col.setValue(new Var(colName, Type.LOB, 0));
    }
    
}
