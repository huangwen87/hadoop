package com.gw.common.jdbc;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.gw.common.Log;
import com.gw.common.hdfs.HdfsUtil;
import com.mysql.jdbc.Statement;


public class LoadDataToMysql {

	public static void main(String[] args)throws Exception {
		String spath = args[0];
		String reg = args[1];
		String mysqlurl = args[2];
		//String mysqlurl = "jdbc:mysql://10.15.107.64:3306/uba?useUnicode=true&characterEncoding=utf8";
		String mysqluser = args[3];
		String mysqlpwd = args[4];
	
		String desttab = args[5];
		String colsplitor = args[6];
		String linesplitor = args[7];
		HdfsUtil fu = HdfsUtil.getInstance();

		//下载HADOOP文件到本地
		List<String> fileList = fu.getFileList(spath,3,reg,1);
		if (fileList == null) {
			Log.error("there are not files in [" + spath + "] to be load to mysql!");
			throw new IOException("there are not files in [" + spath + "] to be load to mysql!");
		}
		Connection conn = null;
		Class.forName("com.mysql.jdbc.Driver").newInstance(); 
		conn = DriverManager.getConnection(mysqlurl,mysqluser, mysqlpwd);
		
		String sql = "LOAD DATA LOCAL INFILE 'sql.csv' REPLACE INTO TABLE "	+ desttab + " fields terminated by '" + colsplitor + "' ESCAPED  BY '\\\\' LINES TERMINATED BY '" + linesplitor + "'";
		FileSystem fs = FileSystem.get(new Configuration());

		for (String fn : fileList) {
			Path sfile = new Path(fn);
			FSDataInputStream fin = fs.open(sfile);
			
			bulkLoadFromInputStream(conn,sql,fin);
		}

	}

	/**
	 *
	 * load bulk data from InputStream to MySQL
	 */
	public static void bulkLoadFromInputStream(Connection cnn,String loadDataSql, InputStream dataStream) throws Exception {
		if(dataStream==null){
			Log.info("InputStream is null ,No data is imported");
			throw new IOException("inputstream is null!");
		}

		Statement myStatement = (com.mysql.jdbc.Statement)cnn.createStatement();

		myStatement.setLocalInfileInputStream(dataStream);
				
		myStatement.execute(loadDataSql);
	}
}
