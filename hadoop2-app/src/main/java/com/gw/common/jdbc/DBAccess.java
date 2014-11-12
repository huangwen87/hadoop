
package com.gw.common.jdbc;

import java.io.Serializable;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

import com.gw.common.Log;

public class DBAccess implements Serializable {

	private static final long serialVersionUID = -5158797661062263924L;

	private Connection cnn = null;

	
	
	public DBAccess (Connection conn){
		cnn = conn;
	}

	
	/**
	 * 执行存储过程，过程不带返回值
	 * @param procedureName
	 * @return -1表示失败，1表示成功
	 * @throws Exception
	 */	
	public int execProcedure(String procedureName){
		int flag = -1;		
		Connection conn = null;
		CallableStatement  cstamt = null;
		try{
			conn = getConnection();
			//			Log.debug("execute Procedure---" + procedureName);
			cstamt = conn.prepareCall("{call "+procedureName+"}");
			cstamt.execute();
			flag = 1;
		}catch(Exception e){
		}finally {
			try{
				cstamt.close();
			}
			catch (Exception e){
				e.printStackTrace();
			}
				
		}
		return flag;
	}
	
	/**
	 * 执行存储过程，过程带返回值(强行规定过程的第一个参数为输出参数,并且返回值为字符串)
	 * @param procedureName
	 * @return
	 * @throws Exception
	 */	
	public String execOutProcedure(String procedureName){
		String str = "";
		Connection conn = null;
		CallableStatement  cstamt = null;
		try{
			conn = getConnection();
			//	Log.debug("execute Procedure---" + procedureName);
			cstamt = conn.prepareCall("{call "+procedureName+"}");
			cstamt.registerOutParameter(1, Types.VARCHAR);
			cstamt.execute();
			str = cstamt.getString(1);
		}catch(Exception e){
			e.printStackTrace();
		}finally {
			try {
				cstamt.close();
			}
			catch (Exception e) {
				System.out.println("Error " + e.getMessage());
				e.printStackTrace();
			}
		}
		return str;
	}
	
	/**
	 * JDBC连接执行LOAD文本文件,列分隔符"/t",行分隔符"/n"
	 * @param tableName 目标表名
	 * @param fileName 文件名，文件内容的顺序必须与表的顺序一致
	 * @return -1表示失败，1 表示成功
	 */
	@SuppressWarnings("static-access")
	public int jdbcLoadData(String tableName,String fileName){
		return this.jdbcLoadData(tableName, fileName, null, null);
	}
	
	/**
	 * JDBC连接执行LOAD文本文件
	 * @param tableName 目标表名
	 * @param fileName 文件名，文件内容的顺序必须与表的顺序一致
	 * @param colSplit 列分隔符，若为null，则用默认值 /t
	 * @param rowSplit 行分隔符，若为null，则用默认值 /n
	 * @return -1表示失败，1 表示成功
	 */
	@SuppressWarnings("static-access")
	public int jdbcLoadData(String tableName,String fileName,String colSplit,String rowSplit){
		List<String> fileList = new ArrayList<String>();
		fileList.add(fileName);
		return this.jdbcLoadData(tableName, fileList, colSplit, rowSplit);
	}
	/**
	 * JDBC连接执行LOAD文本文件
	 * @param tableName 目标表名
	 * @param fileList 本地文件列表，文件内容的顺序必须与表的顺序一致
	 * @param colSplit 列分隔符，若为null，则用默认值 /t
	 * @param rowSplit 行分隔符，若为null，则用默认值 /n
	 * @return -1表示失败，1 表示成功
	 */
	@SuppressWarnings("static-access")
	public int jdbcLoadData(String tableName, List<String> fileList,
			String colSplit, String rowSplit) {
		int result = -1;
		Connection conn =null;
		Statement myStatement = null;
		String csplit = "\t";
		if (null!=colSplit && !"".equals(csplit)){
			csplit = colSplit;
		}
		String rsplit = "\n";
		if (null!=rowSplit && !"".equals(rowSplit)){
			rsplit = rowSplit;
		}
		
		try {
			conn = getConnection();
			myStatement = conn.createStatement();
			for (int i=0;i<fileList.size();i++){
				String sql = "LOAD DATA LOCAL INFILE '" + fileList.get(i) + "' REPLACE INTO TABLE "
					+ tableName + " fields terminated by '" + csplit
					+ "' LINES TERMINATED BY '" + rsplit + "'";
				//	Log.debug("executeUpdate---" + sql);
				boolean re = myStatement.execute(sql);
				if (re){
					result = 1;
				}else{
					result = -1;
				}
			}
		} catch (Exception sqlexception) {
			Log.error("load data2Mysql failed:=================="+sqlexception.toString());
		} finally {
			closeStatement(myStatement);
		}
		return result;
	}
	/**
	 * Run a sql string
	 * @param sql string
	 * @@return -1表示失败，>=0 表示成功(影响的笔数)
	 * @throws SQLException this method
	 */
	@SuppressWarnings("static-access")
	public int executeUpdate(String sql){
		int result = -1;
		Connection conn =null;
		Statement myStatement = null;
		try {
			conn = getConnection();
			myStatement = conn.createStatement();
			Log.debug("executeUpdate---" + sql);
			result = myStatement.executeUpdate(sql);
//			result = 1;
		} catch (Exception sqlexception) {
			Log.error("conndb execute() execute failed:=================="+sql);
		} finally {
			closeStatement(myStatement);
			
		}
		return result;
	}
	/**
	 * jdbc连接执行SQL，用于单支程序编码数据
	 * @param sql string
	 * @@return -1表示失败，>0 表示成功(影响的笔数)
	 * @throws SQLException this method
	 */
	@SuppressWarnings("static-access")
	public int jdbcExecuteUpdate(String sql){
		int result = -1;
		Connection conn =null;
		Statement myStatement = null;
		try {
			conn = getConnection();
			myStatement = conn.createStatement();
			Log.debug("executeUpdate---" + sql);
			result = myStatement.executeUpdate(sql);
//			result = 1;
		} catch (Exception sqlexception) {
			Log.error("conndb execute() execute failed:=================="+sql);
		} finally {
			closeStatement(myStatement);
			
		}
		return result;
	}
	/**
	 * 非事务执行多句SQL
	 * @param sqls
	 * @@return -1表示失败，1表示成功
	 */
	@SuppressWarnings("unchecked")
	public int executeUpdates(List sqls) {
		int flag = -1;
		String sql = "";
		Connection conn =null;
		Statement myStatement = null;
		try {
			conn = getConnection();
			myStatement = conn.createStatement();
			for (Iterator it = sqls.iterator(); it.hasNext();) {
				sql = (String) it.next();
				Log.debug("executeUpdates---" + sql);
				myStatement.executeUpdate(sql);
			}
			flag = 1;
		} catch (SQLException e) {
			Log.error(sql + "  执行有误");
		} finally {
			closeStatement(myStatement);
			
		}
		return flag;
	}
	/**
	 * 非事务执行多句SQL,jdbc连接执行SQL，用于单支程序编码数据
	 * @param sqls
	 * @@return -1表示失败，1表示成功
	 */
	@SuppressWarnings("unchecked")
	public int jdbcExecuteUpdates(List sqls) {
		int flag = -1;
		String sql = "";
		Connection conn =null;
		Statement myStatement = null;
		try {
			conn = getConnection();
			myStatement = conn.createStatement();
			for (Iterator it = sqls.iterator(); it.hasNext();) {
				sql = (String) it.next();
				Log.debug("executeUpdates---" + sql);
				myStatement.executeUpdate(sql);
			}
			flag = 1;
		} catch (SQLException e) {
			Log.error(sql + "  执行有误");
		} finally {
			closeStatement(myStatement);
			
		}
		return flag;
	}
	/**
	 * 事务执行多句SQL(如不成功,则回滚)
	 * @param sqls
	 * @@return -1表示失败，1表示成功
	 */
	@SuppressWarnings({ "unchecked", "static-access" })
	public int executeUpdatesByTran(List sqls){
		int flag = -1;
		String sql = "";
		boolean autoCommit = true;
		int transpri = 0;
		Connection conn =null;
		Statement myStatement = null;
		try {
			conn = getConnection();
			transpri = conn.getTransactionIsolation();
			autoCommit = conn.getAutoCommit();
			conn.setAutoCommit(false);
			conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
			myStatement = conn.createStatement();
			for (Iterator it = sqls.iterator(); it.hasNext();) {
				sql = (String) it.next();
				myStatement.executeUpdate(sql);
				Log.debug("executeUpdatesByTran---" + sql);
			}
			conn.commit();
			flag = 1;
		} catch (SQLException e) {
			e.printStackTrace();
			Log.error("\n"+sql + "  执行有误");
			try {
				conn.rollback();
				conn.setAutoCommit(autoCommit);
			} catch (SQLException e1) {
			}
		} finally {
			try {
				conn.setTransactionIsolation(transpri);
			} catch (SQLException e) {
				e.printStackTrace();
			}
			closeStatement(myStatement);
			
		}
		return flag;
	}
	
	/**
	 * 事务执行多句SQL(如不成功,则回滚),jdbc连接执行SQL，用于单支程序编码数据
	 * @param sqls
	 * @@return -1表示失败，1表示成功
	 */
	@SuppressWarnings({ "unchecked", "static-access" })
	public int jdbcExecuteUpdatesByTran(List sqls){
		int flag = -1;
		String sql = "";
		boolean autoCommit = true;
		int transpri = 0;
		Connection conn =null;
		Statement myStatement = null;
		try {
			conn = getConnection();
			transpri = conn.getTransactionIsolation();
			autoCommit = conn.getAutoCommit();
			conn.setAutoCommit(false);
			conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
			myStatement = conn.createStatement();
			for (Iterator it = sqls.iterator(); it.hasNext();) {
				sql = (String) it.next();
				myStatement.executeUpdate(sql);
				Log.debug("executeUpdatesByTran---" + sql);
			}
			conn.commit();
			flag = 1;
		} catch (SQLException e) {
			e.printStackTrace();
			Log.error("\n"+sql + "  执行有误");
			try {
				conn.rollback();
				conn.setAutoCommit(autoCommit);
			} catch (SQLException e1) {
			}
		} finally {
			try {
				conn.setTransactionIsolation(transpri);
			} catch (SQLException e) {
				e.printStackTrace();
			}
			closeStatement(myStatement);
			
		}
		return flag;
	}
	/**
	 * @param sql 传送所需的sql语句
	 * @return 一个ArrayList，里面存放着一系列的LinkedHashMap, 每个HashMap对应查询结果的一行数据
	 * @throws SQLException 
	 */
	public List<LinkedHashMap<String,String>> getArrayList(String sql){
		return getArrayList(sql,0,0);
	}
	
	/**
	 * 取select的数据中的记录范围的数据
	 * @param sql select语句
	 * @param startRecordNumb 起始记录号，从0开始,>startRecordNumb有效
	 * @param endReordNumb 结束记录号，从0开始,<endReordNumb有效
	 * @return 每笔数据一个map，map的key为select语句中的小写字段名
	 */
	public List<LinkedHashMap<String,String>> getArrayList(String sql,long startRecordNumb,long endReordNumb){
		if (startRecordNumb>endReordNumb ||
				endReordNumb<0)
			return null;
		if (endReordNumb>0 || startRecordNumb>0) {
			//sql +=" limit "+endReordNumb;
			sql +=" limit "+startRecordNumb+","+(endReordNumb-startRecordNumb+1);
		}
		
		Connection conn =null;
		Statement myStatement = null;
		ResultSet rs = null;
		List<LinkedHashMap<String,String>> li = null;
		try {
			Log.debug("get_ArrayList---" + sql);
			conn = getConnection();
			myStatement = conn.createStatement();
			rs = myStatement.executeQuery(sql);
			
			String[] colnames;
			// 得到结果集的相关信息,注意 ResultSetMetaData 中的编号从1开始
			ResultSetMetaData rsmd = rs.getMetaData();
			int columnCount = rsmd.getColumnCount();
			colnames = new String[columnCount];
			for (int i = 1; i <= columnCount; i++) {
				colnames[i - 1] = (rsmd.getColumnLabel(i)).trim();
			}
	        long j=-1;
			li = new ArrayList<LinkedHashMap<String,String>>();
			
			while (rs.next()) {
	        	j++;
//	    		//开始的笔数直接跳过
//	        	if (endReordNumb>0 && j<startRecordNumb)
//	    			continue;
	        	
				LinkedHashMap<String,String> hash = new LinkedHashMap<String,String>();
				for (int i = 1; i <= columnCount; i++) {
					String colname = (colnames[i - 1]).trim().toLowerCase();
					String colvalue = (String) rs.getString(colname);
					hash.put(colname, colvalue);
				}// for end~~
				if(hash.size()>0) li.add(hash);
			}// while end~~
		}catch(SQLException e){
			e.printStackTrace();
			Log.error("SQL语句执行失败。---:"+sql);
			return null;
		}
		return li;
	}


	/**
	 * 得到一条记录,若sql语句返回多条记录，则返回第一条
	 * @param sql 传入的sql语句
	 */
	public LinkedHashMap<String,String> getRow(String sql){
		List<LinkedHashMap<String,String>> list = getArrayList(sql);
		if (list != null && list.size() > 0) {
			return list.get(0);
		} else {
			return null;
		}
	}	
	
	/**
	 * 执行sql_string语句，只选择一个字段返回，字段的名字是sql_col
	 * @param sql_string sql语句
	 * @param sql_col 列名
	 */
	public String getString(String sql_string, String sql_col){
		LinkedHashMap<String,String> hash = getRow(sql_string);
		if (hash != null && hash.size() > 0)
			return hash.get(sql_col);
		else
			return "";
	}


	/**
	 * 取数据库连接
	 * @return
	 */
	public synchronized Connection getConnection() {
		return cnn;
	}

	public void closeStatement(Statement st){
		try {
			if (st != null) {
				st.close();
				st = null;
			}
		} catch (Exception e) {
			//e.printStackTrace();
		}
	}

	public void closeResultSet(ResultSet rs){
		try {
			rs.close();
			rs=null;
		} catch (Exception e) {
			//e.printStackTrace();
		}
	}
		
	/**
	 * 执行查询语句，返回结果、状态，连接，
	 * 注意：使用此方法后，必须调用方法closeAll(List<Object> list)把结果，状态，连接关闭或释放!!!!!!!!!!!!!!
	 * @param sql 查询SQL
	 * @return	返回 result(0) 查询结果,ResultSet;
	 * 				result(1) 查询状态,Statement;
	 * 				result(2) 查询状态,Connection;
	 */
	public List<Object> executeQuery1(String sql){
		Connection myConnection = null;
		Statement myStatement=null;
		ResultSet rs = null;
		try {
			Log.debug("executeQuery---" + sql);
			myConnection = getConnection();
			myStatement = myConnection.createStatement();
			rs = myStatement.executeQuery(sql);
			List<Object> result = new ArrayList<Object>();
			result.add(rs);
			result.add(myStatement);
			result.add(myConnection);
			return result;
		}catch(SQLException e){
			return null;
		} 
	}

	
	
}
