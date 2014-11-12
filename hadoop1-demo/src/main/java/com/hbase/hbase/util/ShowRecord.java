package com.hbase.hbase.util;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class ShowRecord {
	
	private static Configuration hbaseconfig = null;
	static{
		  Configuration conf = new Configuration();
		  conf.set("hbase.zookeeper.quorum","10.15.107.63");
		  conf.set("hbase.zookeeper.property.clientPort","2183");
		  hbaseconfig = HBaseConfiguration.create(conf);
	}
	 
	public static void main(String[] args) {
		 //注意字段查询区分大小写
		  /***
		   * 查询表student
		   */
		  //查询所有
		  //ShowRecord.showAllRecords("newstable"); 
		  
		  //根据主键rowKey查询一行数据
		  //ShowRecord.showOneRecordByRowKey("cf", "time");
		  
		  //根据主键查询某行中的一列
		  //ShowRecord.showOneRecordByRowKey_cloumn("cf", "time","time");
		  ShowRecord.showOneRecordByRowKey_cloumn("newstable", "396026347-2014-04-02 09:16:04.0","news");
		  
		  
	 }
	
	
	 /****
	  * 使用scan查询所有数据
	  * @param tableName
	  */
	 public static void showAllRecords(String tableName)
	 {
		  System.out.println("start==============show All Records=============");
		  
		  HTablePool pool = new HTablePool(hbaseconfig,1000);
		  //创建table对象
		  //HTable table = (HTable) pool.getTable(tableName); //这个强制转换有点问题,可能版本问题
		  HTableInterface table =  pool.getTable(tableName);
		  
		  try {
			   //Scan所有数据
			   Scan scan = new Scan();
			   scan.getMaxVersions();   //获取所有版本的值
			   ResultScanner rss = table.getScanner(scan);
			   
			   for(Result r:rss){
				    System.out.println("\n row: "+new String(r.getRow()));
				    
				    for(KeyValue kv:r.raw()){

					     System.out.println("family=>"+new String(kv.getFamily(),"utf-8")
					           +"  value=>"+new String(kv.getValue(),"utf-8")
					           +"  qualifer=>"+new String(kv.getQualifier(),"utf-8")
					           +"  timestamp=>"+kv.getTimestamp());    
				    }
			   }
			   rss.close();
		  } catch (IOException e) {
		      e.printStackTrace();
		  }  
		  System.out.println("\n end==============show All Records=============");
	 }
	 
	 
	 /***
	  * 根据主键rowKey查询一行数据
	  * get 'student','010'
	  */
	 public static void showOneRecordByRowKey(String tableName,String rowkey)
	 {
		  HTablePool pool = new HTablePool(hbaseconfig,1000);
		  HTableInterface table =  pool.getTable(tableName);
		    
		  try {
			   Get get = new Get(rowkey.getBytes()); //根据主键查询
			   Result r = table.get(get);
			   System.out.println("start===showOneRecordByRowKey==row: "+"\n");
			   System.out.println("row: "+new String(r.getRow(),"utf-8"));
			   
			   for(KeyValue kv:r.raw()){
			    //时间戳转换成日期格式
			    String timestampFormat = new SimpleDateFormat("yyyy-MM-dd HH:MM:ss").format(new Date(kv.getTimestamp()));
			       //System.out.println("===:"+timestampFormat+"  ==timestamp: "+kv.getTimestamp());
			    System.out.println("\nKeyValue: "+kv);
			    System.out.println("key: "+kv.getKeyString());
			    
			    System.out.println("family=>"+new String(kv.getFamily(),"utf-8")
			          +"  value=>"+new String(kv.getValue(),"utf-8")
			    +"  qualifer=>"+new String(kv.getQualifier(),"utf-8")
			    +"  timestamp=>"+timestampFormat);
			 
			   }
		  } catch (IOException e) {
		   // TODO Auto-generated catch block
		   e.printStackTrace();
		  }
		  System.out.println("end===========showOneRecordByRowKey");
	 }
	 
	 /***
	  * 根据主键rowKey  start  end 查询某一区间的数据
	  */
	 public static void showRecordsByBetweenRowKey(String tableName,String rowkey_start, String rowkey_end)
	 {
		  HTablePool pool = new HTablePool(hbaseconfig,1000);
		  HTableInterface table =  pool.getTable(tableName);
		    
		  try {
			   Scan scan = new Scan();
			   scan.setStartRow(rowkey_start.getBytes());
			   scan.setStopRow(rowkey_end.getBytes());
			   scan.setCaching(10000);
			   scan.setCacheBlocks(false);
			   ResultScanner r = table.getScanner(scan);
			   System.out.println("start===showRecordsByBetweenRowKey==row: "+"\n");
			   for(Result result = r.next(); result != null; result = r.next()){
				  byte[] family_qualifier = result.getValue("news".getBytes(), "newsid".getBytes());
				  String m = new String(family_qualifier);
			      System.out.println("key: "+ m);
			   }
		  } catch (IOException e) {
		   // TODO Auto-generated catch block
		   e.printStackTrace();
		  }
		  System.out.println("end===========showRecordsByBetweenRowKey");
	 }
	 
	 
	 
	 /**
	  * 根据rowkey,一行中的某一列簇查询一条数据
	  * get 'student','010','info'  
	  * student sid是010的info列簇（info:age,info:birthday）
	  * 
	  * get 'student','010','info:age'  
	  * student sid是010的info:age列,quafilier是age
	  */
	 //public static void showOneRecordByRowKey_cloumn(String tableName,String rowkey,String column,String quafilier)
	 public static void showOneRecordByRowKey_cloumn(String tableName,String rowkey,String column)
	 {
		  System.out.println("start===根据主键查询某列簇showOneRecordByRowKey_cloumn");
		  
		  HTablePool pool = new HTablePool(hbaseconfig,1000);
		  HTableInterface table =  pool.getTable(tableName);
		    
		  try {
			   Get get = new Get(rowkey.getBytes()); 
			   get.addFamily(column.getBytes()); //根据主键查询某列簇   
			   //get.addColumn(Bytes.toBytes(column),Bytes.toBytes(quafilier)); ////根据主键查询某列簇中的quafilier列
			   Result r = table.get(get);
			   
			   for(KeyValue kv:r.raw()){
				    System.out.println("KeyValue---"+kv);
				    System.out.println("row=>"+new String(kv.getRow()));
				    System.out.println("family=>"+new String(kv.getFamily(),"utf-8")+": "+new String(kv.getValue(),"utf-8"));
				    System.out.println("qualifier=>"+new String(kv.getQualifier())+"\n");
		       }
		   } catch (IOException e) {
		       e.printStackTrace();
		   }
		   System.out.println("end===========showOneRecordByRowKey_cloumn");
	 }
	 
	 
	  
	 //（1）时间戳到时间的转换.单一的时间戳无法给出直观的解释。
	 public String GetTimeByStamp(String timestamp)
	 {
	  long datatime= Long.parseLong(timestamp);
	  Date date=new Date(datatime);
	  SimpleDateFormat format=new SimpleDateFormat("yyyy-MM-dd HH:MM:ss");
	  String timeresult=format.format(date);
	  System.out.println("Time : "+timeresult);
	  return timeresult;
	 
	 }
	 
	 
	 //（2）时间到时间戳的转换。注意时间是字符串格式。字符串与时间的相互转换，此不赘述。
	 public String GetStampByTime(String time)
	 {
	  String Stamp="";
	  SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	  Date date;
	  try
	  {
	   date=sdf.parse(time);
	   Stamp=date.getTime()+"000";
	   System.out.println(Stamp);
	  
	  }catch(Exception e){e.printStackTrace();}
	  return Stamp; 
	 }
}
