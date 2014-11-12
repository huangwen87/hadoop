package com.gw.doubleleven;

import java.io.IOException;
import java.lang.reflect.Field;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import com.gw.common.DelimitDecoder;
import com.gw.common.DelimitEncoder;
import com.gw.common.Log;

public class LogParseMapper extends Mapper<LongWritable, Text, Text, Text>{
	
	private DelimitDecoder decoder1 = new DelimitDecoder(" ",null);
	private DelimitDecoder decoder2 = new DelimitDecoder("&",null);
	private DelimitEncoder encoder = new DelimitEncoder(",",null); 
	
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
	}
	
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		
		String str = value.toString();
		if(str == null)
			return;
		String[] arr  = decoder1.split(str);
		if(arr == null || arr.length !=2)
			return;
		String   date = myDateFormat(arr[0].toString()).substring(0, 8);
		String [] paramArr = decoder2.split(arr[1]);
	    processLogRecord(date, paramArr, context);

		
	}
	
	protected void processLogRecord(String date, String[] arr, Context context){	
	  try{
			String paramName = null;
			String paramValue = null;
			Map<String, String> paramNameList = getAllComponentsName(new ParamModel());
			for(String arrsub : arr){
				int index = arrsub.indexOf("=");
				if(index > 0){  //?存在
					paramName = arrsub.substring(0, index);
					paramValue = arrsub.substring(index+1, arrsub.length());
				}else{
					Log.info("the param of" + arrsub + "is error!!");
					continue;
				}
				if(paramNameList.containsKey(paramName)){
					paramNameList.put(paramName, paramValue);
				}
			}
			contextWrite(date, paramNameList, context);
		}catch(Exception e){
			e.printStackTrace();
		}
	}
	
	protected void processLogRecord2(String date, String[] arr){	
		  try{
				String paramName = null;
				String paramValue = null;
				Map<String, String> paramNameList = getAllComponentsName(new ParamModel());
				for(String arrsub : arr){
					int index = arrsub.indexOf("=");
					if(index > 0){  //?存在
						paramName = arrsub.substring(0, index);
						paramValue = arrsub.substring(index+1, arrsub.length());
					}else{
						Log.info("the param of arrsub is error!!");
						continue;
					}
					if(paramNameList.containsKey(paramName)){
						paramNameList.put(paramName, paramValue);
					}
				}
				for(String o : paramNameList.keySet()){
					System.out.println(date  + ", " + o + ", " + paramNameList.get(o));
				}
			}catch(Exception e){
				e.printStackTrace();
			}
		}
	
	/**
	 * 
	 * 获取参数列表
	 * */
	public Map<String, String> getAllComponentsName(Object f) { 
        // 获取f对象对应类中的所有属性域 
		Map<String, String> result = new HashMap<String, String>();
		try{
		        Field[] fields = f.getClass().getDeclaredFields(); 
		        for(int i = 0 , len = fields.length; i < len; i++) { 
		            // 对于每个属性，获取属性名 
		            String varName = fields[i].getName(); 
		            try { 
		                // 获取原来的访问控制权限 
		                boolean accessFlag = fields[i].isAccessible(); 
		                // 修改访问控制权限 
		                fields[i].setAccessible(true); 
		                result.put(varName, "");
		                // 恢复访问控制权限 
		                fields[i].setAccessible(accessFlag); 
		            } catch (IllegalArgumentException ex) { 
		                ex.printStackTrace(); 
		            }
		         }
		}catch(Exception e){
			e.printStackTrace();
		}
		return result;
    } 
		
	/**
	 * desc: 格式化日期
	 * @param str
	 * @return String
	 * */
	public static String myDateFormat(String str){
		if(str == null)
			return " ";
		String result = new String();
		String pattern = "yyyy-MM-dd'T'HH:mm:ss";
		SimpleDateFormat firstPattern = new SimpleDateFormat(pattern);
		try {
			Date time = firstPattern.parse(str);
			String pattern1 = "yyyyMMddHHmmssSSS";
			SimpleDateFormat secondPattern = new SimpleDateFormat(pattern1);
			result = secondPattern.format(time);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return result;
	}
	
	public void contextWrite(String date, Map<String, String> paramNameList, Context context) throws IOException, InterruptedException{
		if(paramNameList == null || date == null)
			return;
		encoder.reSet();
		encoder.addRecord(date);
		for(String o : paramNameList.keySet()){
			encoder.addRecord(paramNameList.get(o));
		}
		context.write(new Text(date), new Text(encoder.toString()));
	}
	
	
	
	
	
	
	public static void main(String[] args){
		LogParseMapper lpm = new LogParseMapper();
		//System.out.println(lpm.myDateFormat("2014-11-05T14:05:56+08:00"));
		String date = "2014-11-05T14:05:56+08:00";
		String value ="param1=value1&param2=value2&param3=value3&param4=value4&param5=value5";
		String[] arr = value.split("&");
		lpm.processLogRecord2(myDateFormat(date).substring(0, 8), arr);
		
	}
	
	
}
