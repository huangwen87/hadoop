package com.gw.util;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Locale;

public class DateUtil {
	
	/**
	 * yyyy-MM-dd HH:mm:ss
	 */
	public static final String TIME_FORMAT = "yyyy-MM-dd HH:mm:ss";
	
	/**
	 * yyyy-MM-dd
	 */
	public static final String DATE_FORMAT = "yyyy-MM-dd";
	
	/**
	 * oracle中日期格式化字符串
	 */
	public static final String TIME_FORMAT_ORACLE = "yyyy-mm-dd hh24:mi:ss";
	
	/**
	 * 获取服务器当前日期
	 * @return
	 */
	public static Date getCurrentDate() {
		return new Date(System.currentTimeMillis());
	}
	
	/**
	 * 获取服务器当前时间的字符串 ,格式 ：yyyy-MM-dd HH:mm:ss
	 * @return
	 */
	public static String getCurrentDateStr(){
		
		return convertDateToStr(getCurrentDate(), TIME_FORMAT);
	}
	
	/**
	 * 增加天数
	 * @param date
	 * @param days
	 * @return
	 */
	public static Date addDate(Date date, int days) {
		if (date == null) return date;
		Locale loc = Locale.getDefault();
		GregorianCalendar cal = new GregorianCalendar(loc);
		cal.setTime(date);
		int day = cal.get(Calendar.DAY_OF_MONTH);
		int month = cal.get(Calendar.MONTH);
		int year = cal.get(Calendar.YEAR);
		cal.set(year, month, day + days);
		return cal.getTime();
	}
	
	/**
	 * 将字符串转换为日期格式 
	 * @param dateStr
	 * @param dateFormat
	 * @return
	 */
	public static Date convertStrToDate(String dateStr, String dateFormat) {
		if (dateStr == null || dateStr.equals("")) {
			return null;
		}
		SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
		try {
			return sdf.parse(dateStr);
		}
		catch (Exception e) {
			throw new RuntimeException("DateUtil.convertStrToDate():" + e.getMessage());
		}
	}
	
	/**
	 * 将日期转换为字符串格式
	 * @param date
	 * @param dateFormat
	 * @return
	 */
	public static String convertDateToStr(Date date, String dateFormat) {
		if (date == null) {
			return null;
		}
		SimpleDateFormat sdf = new SimpleDateFormat(dateFormat);
		return sdf.format(date);
	}
	
	/**
	 * 给一日期增加一时间
	 * @param timePart HH,mm,ss 
	 * @param number 要增加的时间
	 * @param date 日期对象
	 * @return
	 */
	public static Date addTime(String timePart, int number, Date date) {
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		if (timePart.equals("HH")) {
			cal.add(Calendar.HOUR, number);
		}
		else if (timePart.equals("mm")) {
			cal.add(Calendar.MINUTE, number);
		}
		else if (timePart.equals("ss")) {
			cal.add(Calendar.SECOND, number);
		}
		else {
			throw new IllegalArgumentException("DateUtil.addDate()方法非法参数值：" + timePart);
		}
		return cal.getTime();
	}
	
	/**
	 * 
	 * 清除指定的时间
	 * @param date
	 * @param timePart HH,mm,ss 
	 * @return
	 */
	public static Date clearTime(Date date,String timePart){
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		if (timePart.equals("HH")) {
			cal.clear(Calendar.HOUR);
		}
		else if (timePart.equals("mm")) {
			cal.clear(Calendar.MINUTE);
		}
		else if (timePart.equals("ss")) {
			cal.clear(Calendar.SECOND);
		}
		else {
			throw new IllegalArgumentException("DateUtil.addDate()方法非法参数值：" + timePart);
		}
		return cal.getTime();
	}
	
	public static java.sql.Date convertStrDateToSqlDate(String dateStr, String dateFormat){
		Date date = convertStrToDate(dateStr,dateFormat);
		java.sql.Date sqlDate = new java.sql.Date(date.getTime());
		return sqlDate;
	}
	
	
	/**
	 * 
     *  字符串时间比较
     *  
     *  @return   1--DATE1大    -1---DATE2大    0------相等
	 */
    public static int compare_date(String DATE1, String DATE2) {
        DateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
         try {
             Date dt1 = df.parse(DATE1);
             Date dt2 = df.parse(DATE2);
             if (dt1.getTime() > dt2.getTime()) {
                 return 1;
             } else if (dt1.getTime() < dt2.getTime()) {
                 return -1;
             } else {
                 return 0;
             }
         } catch (Exception exception) {
             exception.printStackTrace();
         }
         return 0;
     }
	
}
