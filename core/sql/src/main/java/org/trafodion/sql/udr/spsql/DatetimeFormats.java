package org.trafodion.sql.udr.spsql;

public enum DatetimeFormats {

	DATETIME_FORMAT_MIN("YYYY-MM-DD"),
		    DATETIME_FORMAT_MIN_DATE("YYYY-MM-DD"),
		    DATETIME_FORMAT_DEFAULT("YYYY-MM-DD"),
		    DATETIME_FORMAT_USA("MM/DD/YYYY AM|PM"),
		    DATETIME_FORMAT_EUROPEAN("DD.MM.YYYY"),   // 
		    DATETIME_FORMAT_DEFAULT2("YYYY-MM"),   // 
		    DATETIME_FORMAT_USA2("MM/DD/YYYY"),       // 
		    DATETIME_FORMAT_USA3("YYYY/MM/DD"),       // 
		    DATETIME_FORMAT_USA4("YYYYMMDD"),       // YYYYMMDD
		    DATETIME_FORMAT_USA5("YY/MM/DD"),       // 
		    DATETIME_FORMAT_USA6("MM/DD/YY"),       // MM/DD/YY
		    DATETIME_FORMAT_USA7("MM-DD-YYYY"),       // MM-DD-YYYY
		    DATETIME_FORMAT_USA8("YYYYMM"),       // YYYYMM
		    DATETIME_FORMAT_EUROPEAN2("DD-MM-YYYY"),  // DD-MM-YYYY
		    DATETIME_FORMAT_EUROPEAN3("DD-MON-YYYY"),  // DD-MON-YYYY
		    DATETIME_FORMAT_EUROPEAN4("DDMONYYYY"),  // DDMONYYYY
		    DATETIME_FORMAT_MAX_DATE("DDMONYYYY"),
		    DATETIME_FORMAT_MIN_TIME("HH24:MI:SS"),
		    DATETIME_FORMAT_TS4("HH24:MI:SS"),
		    DATETIME_FORMAT_MAX_TIME("HH24:MI:SS"),
		    DATETIME_FORMAT_MIN_TS("YYYYMMDDHH24MISS"),
		    DATETIME_FORMAT_TS1("YYYYMMDDHH24MISS"),
		    DATETIME_FORMAT_TS2("DD.MM.YYYY:HH24:MI:SS"),     // DD.MM.YYYY:HH24:MI:SS
		    DATETIME_FORMAT_TS3("YYYY-MM-DD HH24:MI:SS"),     // YYYY-MM-DD HH24:MI:SS
		    DATETIME_FORMAT_TS5("YYYYMMDD:HH24:MI:SS"),     // YYYYMMDD:HH24:MI:SS
		    DATETIME_FORMAT_TS6("MMDDYYYY HH24:MI:SS"),     // MMDDYYYY HH24:MI:SS
		    DATETIME_FORMAT_TS7("MM/DD/YYYY HH24:MI:SS"),     // MM/DD/YYYY HH24:MI:SS
		    DATETIME_FORMAT_TS8("DD-MON-YYYY HH:MI:SS"),     // DD-MON-YYYY HH:MI:SS
		    DATETIME_FORMAT_TS9("MONTH DD, YYYY, HH:MI AM|PM"),     // MONTH DD, YYYY, HH:MI AM|PM
		    DATETIME_FORMAT_TS10("DD.MM.YYYY HH24:MI:SS"),    // DD.MM.YYYY HH24:MI:SS
		    DATETIME_FORMAT_TS11("YYYY/MM/DD HH24:MI:SS"),    // YYYY/MM/DD HH24:MI:SS
		    DATETIME_FORMAT_TS12("YYYY-MM-DD:HH24:MI:SS"),    // YYYY-MM-DD:HH24:MI:SS
		    DATETIME_FORMAT_MAX_TS("YYYY-MM-DD:HH24:MI:SS"),
		    DATETIME_FORMAT_MAX("YYYY-MM-DD:HH24:MI:SS"),
		    DATETIME_FORMAT_MIN_NUM("YYYY-MM-DD:HH24:MI:SS"),
		    DATETIME_FORMAT_NUM1("99:99:99:99"),     // 99:99:99:99
		    DATETIME_FORMAT_NUM2("-99:99:99:99"),     // -99:99:99:99
		    DATETIME_FORMAT_MAX_NUM("-99:99:99:99"),

		    DATETIME_FORMAT_EXTRA_MIN("-99:99:99:99"),
		    DATETIME_FORMAT_EXTRA_HH("hour of day(00-23)"),   // hour of day(00-23)
		    DATETIME_FORMAT_EXTRA_HH12("hour of day(01-12)"), // hour of day(01-12)
		    DATETIME_FORMAT_EXTRA_HH24("hour of day(00-23)"), // hour of day(00-23)
		    DATETIME_FORMAT_EXTRA_MI("minute(00-59)"),   // minute(00-59)
		    DATETIME_FORMAT_EXTRA_SS("second(00-59)"),   // second(00-59)
		    DATETIME_FORMAT_EXTRA_YYYY("year(4 digits)"), // year(4 digits)
		    DATETIME_FORMAT_EXTRA_YYY("year(last 3 digits of year)"),  // year(last 3 digits of year)
		    DATETIME_FORMAT_EXTRA_YY("year(last 2 digits of year)"),   // year(last 2 digits of year)
		    DATETIME_FORMAT_EXTRA_Y("year(last digit of year)"),    // year(last digit of year)
		    DATETIME_FORMAT_EXTRA_MON("month(3 chars in English)"),  // month(3 chars in English)
		    DATETIME_FORMAT_EXTRA_MM("month(01-12)"),   // month(01-12)
		    DATETIME_FORMAT_EXTRA_DY("name of day(3 chars in English) exp. SUN"),   // name of day(3 chars in English) exp. SUN
		    DATETIME_FORMAT_EXTRA_DAY("name of day"),  // name of day,padded with blanks to length of 9 characters. exp. SUNDAY
		    DATETIME_FORMAT_EXTRA_CC("century"),   // century
		    DATETIME_FORMAT_EXTRA_D("day of week"),    // day of week(Sunday(1) to Saturday(7))
		    DATETIME_FORMAT_EXTRA_DD("day of month");   // day of month(01-31)
		    /*DATETIME_FORMAT_EXTRA_DDD(""),  // day of year(1-366)
		    DATETIME_FORMAT_EXTRA_W(""),    // week of month(1-5)
		    DATETIME_FORMAT_EXTRA_WW(""),   // week number of year(1-53)
		    DATETIME_FORMAT_EXTRA_J(""),    //number of days since January 1, 4713 BC
		    DATETIME_FORMAT_EXTRA_Q(""),    // the quarter of year(1-4)
		    DATETIME_FORMAT_EXTRA_MAX = DATETIME_FORMAT_EXTRA_Q(""),
		    // the following are intended for binder time resolution based 
		    // on operand type to one of the formats above
		    DATETIME_FORMAT_MIN_UNRESOLVED = DATETIME_FORMAT_EXTRA_MAX(""),
		    DATETIME_FORMAT_UNSPECIFIED(""),  // Default format for TO_CHAR; resolved at bind time
		                                  // based on the datatype of the operand
		    DATETIME_FORMAT_MAX_UNRESOLVED = DATETIME_FORMAT_UNSPECIFIED(""),

		    DATETIME_FORMAT_DATE_STR(""), // format in str
		    DATETIME_FORMAT_TIME_STR(""), // format in str
		    DATETIME_FORMAT_NONE(""),
		    DATETIME_FORMAT_ERROR     = -1*/
		    
	private String format;

	private DatetimeFormats(String format) {
		this.format = format;
	}

	public String getFormat() {
		return format;
	}
	 
}
