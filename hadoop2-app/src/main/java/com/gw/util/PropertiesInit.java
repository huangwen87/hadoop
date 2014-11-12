package com.gw.util;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

import org.apache.log4j.Logger;

/**
 *  Properties 初始化 
 *  具体配置可以根据自己想加载的.properties文件来
 *  @author darwen
 *  2013-12-19  下午2:46:46
 */
public class PropertiesInit {
	
	static private Logger log = Logger.getLogger(PropertiesInit.class.getName());
	
	private Properties pro = new Properties();
	
	public Properties getPro() {
		return pro;
	}

	public void setPro(Properties pro) {
		this.pro = pro;
	}

	public PropertiesInit(String path){
		
		InputStream fis;
		try {
			//四个反斜杠代表一个反斜杠, 避免操作系统不同的干扰
			//String workplace = System.getProperty("user.dir").replaceAll("\\\\", "/"); 
			//注意：这里使用this.getClass.getClassLoader()方法而没有使用ClassLoader
			//fis = this.getClass().getClassLoader().getResourceAsStream("plugins/conf/config.properties");
			String base = System.getProperty("user.dir").replaceAll("\\\\", "/");
			File file = new File(base + "/" + path);
			fis = new BufferedInputStream(new FileInputStream(file));
			pro.load(fis);
		} catch (FileNotFoundException e) {
			log.error("the file of ["+ path + "] is not find");
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

}
