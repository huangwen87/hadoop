package com.hbase.hbase.performancetest;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.log4j.Logger;


import com.util.PropertiesInit;

public class DBA {
	
	static private Logger log = Logger.getLogger(DBA.class.getName());

	private static Connection conn = null;
	private static Properties pro = null;
	
	public DBA(){
		init();
	}
	
	public static void init(){
		try {
			PropertiesInit propertiesInit = new PropertiesInit("dba.properties");
			pro = propertiesInit.getPro();
			DriverManager.registerDriver(new com.microsoft.sqlserver.jdbc.SQLServerDriver());
			conn = DriverManager.getConnection(pro.getProperty("sqlserver1.jdbcUrl"),
					pro.getProperty("sqlserver1.user"),
					pro.getProperty("sqlserver1.password"));
			log.info("dba connection successful!");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	
	/**
	 * Desc: 调用存储过程并获取结果
	 * @param begin	自增id的开始值
	 * @param end	自增id的结束值
	 * @return
	 * @throws SQLException
	 * @return List<Model>
	 */
	public List<Model> getCallableStatment(int begin, int end) throws SQLException{
		if(begin > end){
			return null;
		}
		List<Model> result = new ArrayList<Model>();
		try {
			CallableStatement c = conn.prepareCall("{call usp_NewsExtractForEmotionByID(?,?)}");
			c.setInt(1, begin);
			c.setInt(2, end);
			c.execute();
			ResultSet rs = c.getResultSet();
			while(rs.next()){
				Model model = new Model();
				model.setNewsid(rs.getLong("newsid"));
				model.setID(rs.getInt("ID"));
				model.setPublishdate(rs.getString("publishdate"));
				model.setNewstype(rs.getString("newstype"));
				model.setTitle(rs.getString("title"));
				model.setText(rs.getString("text"));
			    result.add(model);
			}
			c.close();
			rs.close();
		} catch (Exception e) {
		   e.printStackTrace();
		} finally{
          conn.close();
		}
		return result;
	}
	
	

}
