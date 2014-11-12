/**
 * 20130304 add by LIANGHS
 * 公用调用Log4j输出日志
 */
package com.gw.common;


public class Log {
	private static org.apache.commons.logging.Log log = org.apache.commons.logging.LogFactory.getLog("");

	/**
	 * 调试信息
	 * @param param	信息内容
	 */
	public static void debug(Object param) {
		log.debug(param);
	}
	
	/**
	 * 一般信息
	 * @param param	信息内容
	 */
	public static void info(Object param) {
		log.info(param);
	}

	/**
	 * 警告信息
	 * @param param	信息内容
	 */
	public static void warn(Object param) {
		log.warn(param);
	}

	/**
	 * 错误信息
	 * @param param	信息内容
	 */
	public static void error(Object param) {
		log.error(param);
	}

	/**
	 * 致命错误信息
	 * @param param	信息内容
	 */
	public static void fatal(Object param) {
		log.fatal(param);
	}
	
	/**
	 * 一般信息，同info()
	 * @param param	信息内容
	 */
	public static void out(Object param) {
		log.info(param);
	}
}