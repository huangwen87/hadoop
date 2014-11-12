package com.gw.common;

/**
 * @date Mon Feb 10 13:36:16 2014
 * @author gumeng
 * @description decode for the message delimit by specified delimiter
 */


import java.lang.*;
import java.util.ArrayList;
import java.io.IOException;

public class DelimitDecoder {
    private String delimiters;

	private String escaper = "\\";
	private boolean useEnclosor;
	private String enclosor = null;

	public DelimitDecoder(String delim,String enclosor) {
		delimiters = delim;

		if (enclosor != null && !enclosor.equals("")) {
			useEnclosor = true;
			this.enclosor = enclosor;
		}
	
    }
	public String[] split (String str) throws IOException {
		Decoder decoder = new Decoder(str);
		ArrayList<String> array = new ArrayList<String>();
		int a = 0;
		while(decoder.hasMoreRecord()){
			array.add(decoder.nextRecord());
		}
		String[] result = new String[array.size()];
		return array.toArray(result);
	}
	class Decoder{
		int currentPosition;
		int del_pos;
		int maxPosition;
		boolean need_pad;
		String str;
		public Decoder(String str)
		{
			currentPosition = 0;
			del_pos = str.length();
			maxPosition = str.length();
			this.str = str;
		}
		public boolean hasMoreRecord() throws IOException {
			if (currentPosition >= maxPosition &&del_pos < maxPosition) {
				currentPosition = maxPosition;
				del_pos = maxPosition;
				return true;
			}

			
			if(currentPosition >= str.length()){
				return false;
			}
			del_pos = currentPosition;		
			if (useEnclosor) {
				if (str.startsWith(enclosor,currentPosition)) {
					del_pos += enclosor.length();
				}
				else
					throw new IOException("illegal format!");
			}
	
			int len = str.length();
			boolean bescap = false;
			boolean bsucc;
			if (useEnclosor)
				bsucc = false;
			else
				bsucc = true;

			while(del_pos < len ){
				if (useEnclosor)
					bsucc = false;
				else
					bsucc = true;
				if (useEnclosor && !bescap && str.startsWith(enclosor,del_pos)) {
					bsucc = true;
					del_pos += enclosor.length();
					if (del_pos >= len) {
						break;
					}

					if (str.startsWith(delimiters + enclosor,del_pos)) {
						//						del_pos += delimiters.length();
					}
					else{
						throw new IOException("illegal str:" + str);
					}
					
					break;
				}

				if (!useEnclosor && !bescap && str.startsWith(delimiters,del_pos)) {
					//del_pos += delimiters.length();
					bsucc = true;
					break;
				}
				if (str.startsWith(escaper,del_pos)) {
					bescap = true;
				}
				else
					bescap = false;
				del_pos ++;
			}
			return bsucc;
		}

		/**
		 * Returns the next token from this string tokenizer.
		 *
		 * @return     the next token from this string tokenizer.
		 * @exception  NoSuchElementException  if there are no more tokens in this
		 *               tokenizer's string.
		 */
		public String nextRecord()throws IOException {
			/* 
			 * If next position already computed in hasMoreElements() and
			 * delimiters have changed between the computation and this invocation,
			 * then use the computed value.
			 */
			int start = currentPosition;
			
			int end = del_pos;
			if (useEnclosor) {
				start += enclosor.length();
				end -= enclosor.length();
			}
			else{
				if(start == end && start == maxPosition)
					return "";
			}

			if (end > str.length()||end < 0) {
				throw new IOException("no more record!");
			}
			if (del_pos < maxPosition) {
				currentPosition = del_pos + delimiters.length();
			}
			else
				currentPosition = del_pos;

			return  escape(str.substring(start,end));
		}

		private String escape (String record)throws IOException {
			int len = record.length();

			String sre = "";
			int i = 0;
			while(i < len){
				if(record.startsWith(escaper,i)){
					if (useEnclosor && record.startsWith(enclosor,i + escaper.length())) {
						sre = sre + enclosor;
						i = i + escaper.length() + enclosor.length();
					}
					else if(!useEnclosor && record.startsWith(delimiters,i + escaper.length())){
						sre = sre + delimiters;
						i = i + escaper.length() + delimiters.length();
					
					}
					else if(record.startsWith(escaper,escaper.length() + i)){
						sre = sre + escaper;
						i = i + 2*escaper.length();
					}
					else
						throw new IOException("illegal format");
				}
				else{
					sre = sre + record.substring(i,i + 1);
					i ++;
				}
				
			}
			return sre;
		
		}
	}
	public static void main(String[] args)throws Exception {
		DelimitDecoder decoder = new DelimitDecoder("&",null);
//		String[] rs = decoder.split(",20140313020052,0,101,865C0C0B4AB0E063E5CAA3387C1A8741,i,");
		String[] rs = decoder.split("abc=b&bc=d");
		for (String str : rs) {
			System.out.println(str);
			
		}
		
		
//		DelimitDecoder decoder1 = new DelimitDecoder(",","\"");
//		rs = decoder1.split("\"\",\"1\",\"2\",\"\",\"34\",\"\"");
//		for (String str : rs) {
//			System.out.println(str);
//			
//		}
//
//		System.out.println("$$$haha");
		

		

	}

}
	
