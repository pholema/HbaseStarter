package com.pholema.tool.starter.hbase.utils;


import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.TimeZone;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DateUtil {
	private static final Logger logger = LoggerFactory.getLogger(DateUtil.class);

    private final static String dateFormatStr = "yyyy-MM-dd HH:mm:ss.SSS";
    private final static String dateFormatStrYYYYMMDD = "yyyyMMdd";
    private final static String dateFormatStrHH = "HH";
    private final static String dateFormatStrmm = "mm";

    public static final DateFormat format = new SimpleDateFormat(dateFormatStr);
    public static final DateFormat formatYYYYMMDD = new SimpleDateFormat(dateFormatStrYYYYMMDD);
    public static final DateFormat formatHH = new SimpleDateFormat(dateFormatStrHH);
    public static final DateFormat formatmm = new SimpleDateFormat(dateFormatStrmm);

    public static String toDateStr(long date) {
        return toDateStr(new Date(date));
    }

    public static String toDateStr(Date date) {
        return format.format(date);
    }
    
    public static String toDateStrYYYYMMDD(long date) {
        return toDateStrYYYYMMDD(new Date(date));
    }

    public static String toDateStrYYYYMMDD(Date date) {
        return formatYYYYMMDD.format(date);
    }
    
    public static String toDateStrHH(long date) {
        return toDateStrHH(new Date(date));
    }

    public static String toDateStrHH(Date date) {
        return formatHH.format(date);
    }
    
    public static String toDateStrmm(long date) {
        return toDateStrmm(new Date(date));
    }

    public static String toDateStrmm(Date date) {
        return formatmm.format(date);
    }
	
    public static Date GMT2Local(Long dateLng){
		SimpleDateFormat gmt_fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		gmt_fmt.setTimeZone(TimeZone.getTimeZone("GMT"));
		SimpleDateFormat loc_fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		loc_fmt.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
		Date r=null;
		try {
			r=loc_fmt.parse(gmt_fmt.format(new Date(dateLng)));
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return r;
	}
	
	
	public static Date toAmericaDate(Long dateLng){
		SimpleDateFormat loc_fmt = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		loc_fmt.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
		Date r=null;
		try {
			r=loc_fmt.parse(loc_fmt.format(new Date(dateLng)));
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return r;
	}
	
	
	public static Date getDateStart (Integer offset) {
		Calendar cal = Calendar.getInstance();
        cal.add(Calendar.DATE,offset);
        cal.set(Calendar.HOUR_OF_DAY,0);
        cal.set(Calendar.MINUTE,0);
        cal.set(Calendar.SECOND,0);
        cal.set(Calendar.MILLISECOND,0);
        return cal.getTime();
    }
	
	
	public static Date getDate (String dateString) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Date date = sdf.parse(dateString);
        return date;
    }
	public static Date getDate (String dateString,String fmtString) throws ParseException {
		SimpleDateFormat sdf = new SimpleDateFormat(fmtString);
		Date date = sdf.parse(dateString);
        return date;
    }
	
	
	public static String getCurrentDateTime(String fmtString){
		SimpleDateFormat fmt = new SimpleDateFormat(fmtString);
		fmt.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
		String curTime=fmt.format(new java.util.Date().getTime()).trim();
		return curTime;
	}
	
	public static String getDateTime(Date now,String fmtString){
		SimpleDateFormat fmt = new SimpleDateFormat(fmtString);
		fmt.setTimeZone(TimeZone.getTimeZone("America/Los_Angeles"));
		String curTime=fmt.format(now.getTime()).trim();
		return curTime;
	}
	
	// getMonth(-13) ,return -13 month~ this month
	public static List<String> getMonths (Integer offset) {
		List<String> list = new ArrayList<String>();
		for(int i=0;i<Math.abs(offset);i++){
			Calendar cal = Calendar.getInstance();
			cal.add(Calendar.MONTH,i+offset+1);
			list.add(getDateTime(cal.getTime(),"yyyy-MM"));
		}
        return list;
    }
	
	public static List<String> getDayTimes (Integer offset) {
		String fmtString = "yyyy-MM-dd";
		List<String> list = new ArrayList<String>();
		for(int i=0;i<Math.abs(offset);i++){
			Calendar cal = Calendar.getInstance();
			cal.add(Calendar.DATE,i+offset+1);
			
			//time string
			try {
				String day = getDateTime(cal.getTime(),fmtString);
				list.add(String.valueOf(getDate(day,fmtString).getTime()));
			} catch (ParseException e) {
				e.printStackTrace();
			}
			//date string
			//list.add(getDateTime(cal.getTime(),fmtString));
		}
        return list;
    }
	
}
