package com.gw.util;

import java.util.Properties;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

/**
 * 自定义日志配置文件，方便扩展
 * 
 */
public class LogUtil {

	static {
		try {
			Properties prop = new Properties();
			prop.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("mylog4j.properties"));
			PropertyConfigurator.configure(prop);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/**
	 * 依据不同类的Class对象创建不同的Logger类对象
	 * 
	 * @param clazz
	 * @return
	 */
	public static Logger getInstance(Class<?> clazz) {
		if (clazz == null)
			return null;
		return Logger.getLogger(clazz);
	}

}
