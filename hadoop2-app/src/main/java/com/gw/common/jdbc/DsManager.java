
package com.gw.common.jdbc;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSourceFactory;

public class DsManager {

	private DataSource dataSoruce = null;
	private int inUsed=0;    //使用的连接数
	private String sign = "";
	private Properties  pro=null;

	static public DsManager instance; // 唯一实例
	private  DsManager() {
	}
	
	public static DsManager getInstance() {
		if (instance == null) {
			instance = new DsManager();
		}
		return instance;
	}
	
	/**
	 * 释放连接
	 * @param conn
	 * @throws SQLException
	 */
	public void freeConnection(Connection conn) throws SQLException {
		if (!conn.isClosed()){
			conn.close();
			this.inUsed--;
			}
	}
	
	/**
	 * 设置数据源
	 */
	public void setupDataSource() {
		if (getDataSoruce() == null) {
			try {
				dataSoruce = BasicDataSourceFactory.createDataSource(getProperties());
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	
	public DataSource getDataSoruce() {
		return dataSoruce;
	}

	public void setDataSoruce(DataSource dataSoruce) {
		this.dataSoruce = dataSoruce;
	}
	/**
	 * 取连接
	 * @return
	 * @throws SQLException
	 */
	public synchronized Connection getConnection() throws SQLException {

		Connection conn = null;
		try {
			setupDataSource();
			conn = this.getDataSoruce().getConnection();
		} catch (Exception e) {
			throw new SQLException("Cannot get connection!" + e.getMessage());
		}
		if (conn != null) {
			if (Integer.parseInt((String) getProperties().get("maxActive")) == 0
					|| Integer.parseInt((String) getProperties().get("maxActive")) < this.inUsed) {
				conn.close();
				conn = null;// 达到最大连接数，暂时不能获得连接了。
			}else{
				this.inUsed++;
			}
		}
		if (conn == null)
			throw new SQLException("Cannot get connection!");

		return conn;
	}
	/**
	 * 取连接
	 * @return
	 * @throws SQLException
	 */
	public synchronized Connection getJdbcConnection() throws SQLException {
		Connection conn = null;
		try {
			getProperties();
			for (int i=0;i<50;i++){//试50次
				Class.forName(pro.getProperty("driverClassName"));
				conn = DriverManager.getConnection(pro.getProperty("url"), 
						pro.getProperty("username"), pro.getProperty("password"));
				if (conn!=null) 
					break;
				else
					Thread.sleep(2000);
			}
		} catch (Exception e) {
			throw new SQLException("Cannot get connection!" + e.getMessage());
		}
		if (conn != null) {
			if (Integer.parseInt((String) getProperties().get("maxActive")) == 0
					|| Integer.parseInt((String) getProperties().get("maxActive")) < this.inUsed) {
				conn.close();
				conn = null;// 达到最大连接数，暂时不能获得连接了。
			}else{
				this.inUsed++;
			}
		}
		if (conn == null)
			throw new SQLException("Cannot get connection!");

		return conn;
	}

	public String getSign() {
		return sign;
	}

	public void setSign(String signName) {
		sign = signName;
	}
	
	/**
	 * 加载JDBC配置参数
	 * @return
	 */
	public Properties getProperties() {
		if (pro!=null) return pro;
		pro = new Properties();
		boolean f=true;
		try {
			//pro.load(new FileInputStream(new File(java.net.URLDecoder.decode(DsManager.class.getResource("/jdbc.properties").toString(),"utf-8").replaceAll("file:/", ""))));
			pro.load(DsManager.class.getResourceAsStream("/jdbc.properties"));
			System.out.println("读取配置成功： " + pro.getProperty("url"));
		} catch (FileNotFoundException e) {
			f=false;
			e.printStackTrace();
		} catch (UnsupportedEncodingException e) {
			f=false;
			e.printStackTrace();
		} catch (IOException e) {
			f=false;
			e.printStackTrace();
		}
		if(!f){		
			System.out.println("******读取数据库配置文件失败，使用默认连接：******");	
			pro.setProperty("maxActive", "10");
			pro.setProperty("maxIdle", "100");
			pro.setProperty("maxWait", "200");
			pro.setProperty("driverClassName", "com.mysql.jdbc.Driver");
			//注意：设置MYSQL的配置文件mysql.ini的default-character-set=gb2312
			pro.setProperty("url", "jdbc:mysql://10.15.107.64:3306/uba?useUnicode=true&amp;characterEncoding=gb2312");
			pro.setProperty("username", "root");
			pro.setProperty("password", "111111");
		}else{
		}
		return pro;
	}
}
