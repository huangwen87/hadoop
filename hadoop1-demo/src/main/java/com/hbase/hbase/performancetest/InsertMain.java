package com.hbase.hbase.performancetest;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.hbase.client.Put;
import org.apache.log4j.Logger;

import com.hbase.hbase.util.InsertRecord;

/**
 * desc： 插入数据不会因为hbase已有的数据量大而时间变长
 * @author darwen
 * 2014-4-9 下午4:24:27
 */
public class InsertMain {
	
    static private Logger log = Logger.getLogger(InsertMain.class.getName());

	public static void main(String[] args) throws Exception {
		
		//writeOneFamilyManyColumn();
		writeTwoFamilyManyColumn();   //

	}
	
	
	/**
	 * HashMap 换成  treeMap
	 * 
	 * */
	public static void writeTwoFamilyManyColumn(){
		int startID = 20518612;
		List<Model> list = null;
		List<Put> listPut = new ArrayList<Put>();
		int total = 0;
		try{
			while(total <= 1000000){
				list = new DBA().getCallableStatment(startID, startID += 500);
				log.info("count: " + list.size());
				for(Model model: list){
					int flag = 0;
					String rowkey    = model.getNewsid() + "-" + model.getPublishdate();
					TreeMap<String, String> map =  new TreeMap<String, String>();
					map.put("newsid", Long.toString(model.getNewsid()));
					map.put("ID", Integer.toString(model.getID()));
					map.put("publishdate", model.getPublishdate());
					map.put("newstype", model.getNewstype());
					map.put("title", model.getTitle());
                    map.put("text", model.getText());
					
					InsertRecord insertRecord = new InsertRecord();
					for(String qr : map.keySet()){
						flag++;
						String qualifier = qr;
					    String value     = map.get(qr);
					    Put put = null;
					    if(flag <= 3){
					       put = insertRecord.addRecord("newstable1", rowkey, "news", qualifier, value);
					    }else{
					       put = insertRecord.addRecord("newstable1", rowkey, "newsid", qualifier, value); 
					    }
					    listPut.add(put);
					    if(listPut.size() == 1000){
					    	long all = System.currentTimeMillis();
					    	insertRecord.addBatchRecord("newstable1",listPut);
					    	long end = System.currentTimeMillis();
					    	log.info("dba have: " + total +"  |  time using " + (end - all)*1.0/1000+"s");
					    	listPut.clear();
					    }
					}
			    }
				total += list.size();
				list.clear();
		   }
		}catch(Exception e){
			e.printStackTrace();
		}
		
}
	
	
	
	
	/**
	 * 调用"存储过程"获取数据   每次获取500个(因为那边数据库存储限制) 并插入hbase
	 * 属于 "插入一列簇  多个列"
	 * */
	public static void writeOneFamilyManyColumn(){
			int startID = 20518612;
			List<Model> list = null;
			List<Put> listPut = new ArrayList<Put>();
			int total = 0;
			try{
				while(total <= 1000000){
					list = new DBA().getCallableStatment(startID, startID += 500);
					log.info("count: " + list.size());
					for(Model model: list){
						String rowkey    = model.getNewsid() + "-" + model.getPublishdate();
						Map<String, String> map =  new HashMap<String, String>();
						map.put("newsid", Long.toString(model.getNewsid()));
						map.put("ID", Integer.toString(model.getID()));
						map.put("publishdate", model.getPublishdate());
						map.put("newstype", model.getNewstype());
						map.put("title", model.getTitle());
						map.put("text", model.getText());
						
						InsertRecord insertRecord = new InsertRecord();
						for(String qr : map.keySet()){
							String qualifier = qr;
						    String value     = map.get(qr);
						    Put put = insertRecord.addRecord("newstable1", rowkey, "news", qualifier, value);
						    listPut.add(put);
						    if(listPut.size() == 1000){
						    	long all = System.currentTimeMillis();
						    	insertRecord.addBatchRecord("newstable1",listPut);
						    	long end = System.currentTimeMillis();
						    	log.info("dba have: " + total +"  |  time using " + (end - all)*1.0/1000+"s");
						    	listPut.clear();
						    }
						}
				    }
					total += list.size();
					list.clear();
			   }
			}catch(Exception e){
				e.printStackTrace();
			}
			
	}
	
	

}
