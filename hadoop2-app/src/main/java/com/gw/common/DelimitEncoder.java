package com.gw.common;

/**
 * @date Mon Feb 10 13:48:43 2014
 * @author gumeng
 * @description decode for the message code in specified delimitor or and enclosor
 */

import java.lang.String;
import java.util.ArrayList;
public class DelimitEncoder  {

	private String delimitor;
	private String enclosor;
	private boolean useEnclosor;
	private String str = null;
	private String escaper = "\\";
	
	public DelimitEncoder (String delimitor,String enclosor){
		this.delimitor = delimitor;
		if (enclosor == null || enclosor.equals("")) {
			useEnclosor = false;
		}
		else{
			useEnclosor = true;
			this.enclosor = enclosor;
		}
				
	}
	public String escape (String record) {
		int len = record.length();
		String sre = "";
		int i = 0;
		while(i < len){
			if(record.startsWith(escaper,i)){
				sre = sre + escaper + escaper;
				i += escaper.length();
			}
			else if(useEnclosor && record.startsWith(enclosor,i)){
				sre = sre + escaper + enclosor;
				i += enclosor.length();
			}
			else if(!useEnclosor && record.startsWith(delimitor,i)){
				sre = sre + escaper + delimitor;
				i += delimitor.length();
			}
				else{
				sre = sre + record.substring(i,i + 1);
				i ++;
			}
				
		}
		
		return sre;
	}

	public void addRecord (String record) {
		String tstr = escape(record);
		if (useEnclosor) {
			tstr = enclosor + tstr + enclosor;
		}
		if (str == null) {
			str = tstr;
		}
		else{
			str = str + delimitor + tstr;
		}
	}

	public void reSet () {
		str = null;
	}

	public String toString () {
		return str;
	}
	public static void main(String[] args)throws Exception {

		ArrayList<String> array = new ArrayList<String>();
		array.add("xiaochen\"");
		array.add(",xiaochen");
		array.add("\\sbility");
		array.add("几陈sfsg\\,");


		
		DelimitEncoder encoder = new DelimitEncoder(",",null);
		for (String str : array) {
			encoder.addRecord(str);
		}

		System.out.println(encoder.toString());
		DelimitDecoder decoder = new DelimitDecoder(",",null);
		String[] rs = decoder.split(encoder.toString());
		if (rs.length != array.size()) {
			System.out.println("size not match");
		}
		for (int i = 0; i < rs.length; i++) {
			if (!rs[i].equals(array.get(i))) {
				System.out.println("i:"+i + " not match");
				
			}
			else
				System.out.println("match:" + i + rs[i]);
			

		}

	}

}
