package com.hbase.hbase.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;


public class InsertRecord {

	private static Configuration hbaseconfig = null;
	static{
		  Configuration conf = new Configuration();
		  conf.set("hbase.zookeeper.quorum","10.15.107.63");
		  conf.set("hbase.zookeeper.property.clientPort","2183");
		  hbaseconfig = HBaseConfiguration.create(conf);
	}
    
    /**
     * 创建一张表
     * */
    public static void createTable(String tablename, String[] cfs) throws Exception {
    	HBaseAdmin admin = new HBaseAdmin(hbaseconfig);
    	if(admin.tableExists(tablename)){
    		System.out.println("table has existed.");
    	}
    	else{
    		HTableDescriptor tableDesc = new HTableDescriptor(tablename);
    		for(int i =0; i < cfs.length; i++){
    			/**
    			 * tableDesc.addFamily(new HColumnDescriptor(cfs[i]).setInMemory(true));
    			 * 设置该列缓存到内存中
    			 * setMaxVersions 指定数据最大保存的版本个数。默认为3
    			 * .setBloomFilterType(StoreFile.BloomType.ROW)   设置bloomFilter过滤
    			 * */
    			tableDesc.addFamily(new HColumnDescriptor(cfs[i]));
    			//tableDesc.addFamily(new HColumnDescriptor(cfs[i]).setBloomFilterType(StoreFile.BloomType.ROW));
    		}
    		admin.createTable(tableDesc);
    		System.out.println("create table successfully.");
    	}
    }
    
    /**
     * 添加一条数据
     */
//    public void addRecord (String tableName, String rowKey, String family, String qualifier, String value)  
//            throws Exception{  
//        try {  
//            HTable table = new HTable(hbaseconfig, tableName);
//            Put put = new Put(Bytes.toBytes(rowKey));
//            put.add(Bytes.toBytes(family),Bytes.toBytes(qualifier),Bytes.toBytes(value));  
//            table.put(put);  
//            //System.out.println("insert recored " + rowKey + " to table " + tableName +" ok.");  
//        } catch (IOException e) {  
//            e.printStackTrace();  
//        }  
//    }
//    
    
    /**
     * 添加一条数据的put
     */
    public Put addRecord (String tableName, String rowKey, String family, String qualifier, String value)  
            throws Exception{  
            Put put = new Put(Bytes.toBytes(rowKey));
            put.add(Bytes.toBytes(family),Bytes.toBytes(qualifier),Bytes.toBytes(value));
            return put;
            //System.out.println("insert recored " + rowKey + " to table " + tableName +" ok.");  
    }
    
    
    
    
    
    /**
     * 批量插入数据
     * */
    public  void addBatchRecord(String tableName, List<Put> list){
        try {
        	HTable table = new HTable(hbaseconfig, tableName);
			table.put(list);
			//table.setAutoFlush(false);
			//table.setWriteBufferSize(1024*1024*2);
			//System.out.println("autoFlush: " + table.isAutoFlush());
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
      
    }
    
    
    /** 
     * 删除一行记录 
     */  
    @SuppressWarnings({ "rawtypes", "unchecked" })
	public static void delRecord (String tableName, String rowKey) throws IOException{  
    	try{
		    HTable table = new HTable(hbaseconfig, tableName);  
	        List list = new ArrayList();  
	        Delete del = new Delete(rowKey.getBytes());  
	        list.add(del);  
	        table.delete(list);  
	        System.out.println("del recored " + rowKey + " ok.");  
    	}catch(IOException e){
    		e.printStackTrace();
    	}
       
    }  
    
    
    /** 
     * 删除一张表 
     * @param tableName 
     */  
    public static void dropTable(String tableName) {  
        try {  
            HBaseAdmin admin = new HBaseAdmin(hbaseconfig); 
            if(admin.tableExists(tableName)){
	            admin.disableTable(tableName);  
	            admin.deleteTable(tableName);
	            System.out.println("deleted successfully.");
            }else{
            	 System.out.println("talbe:[" + tableName + "] is not exist, deleted failure.");
            }
        } catch (MasterNotRunningException e) {  
            e.printStackTrace();  
        } catch (ZooKeeperConnectionException e) {  
            e.printStackTrace();  
        } catch (IOException e) {  
            e.printStackTrace();  
        }  
  
    }  
    
    
	
	public static void main(String[] args) {
        try {
        	//createTable("newstable1", new String[]{"news","newsid"});
        	//addRecord("testTable1", "row1", "news", "text", "infomation新闻内容测试！");
        	//dropTable("dsinfo");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
