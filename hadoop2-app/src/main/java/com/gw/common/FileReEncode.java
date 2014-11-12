package com.gw.common;

import java.lang.String;
import java.io.IOException;
import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

import java.io.OutputStreamWriter;
import java.io.InputStreamReader;


public class FileReEncode {
	private static int BUF_SIZE = 100*1024;
	
	public static void reEncode (InputStream is,String sencode,OutputStream os,String dencod)throws IOException {
		if (sencode.equals(dencod)) {
			throw new IOException("文件编码不需要改变！");
		}
				
		InputStreamReader isr = new InputStreamReader(is,sencode);
		
		OutputStreamWriter osw = new OutputStreamWriter(os,dencod);
		char[] buf = new char[BUF_SIZE];

		int read;
		while( (read = isr.read(buf,0,BUF_SIZE)) != -1 ){
			osw.write(buf,0,read);
		}
		osw.close();
	}

}
