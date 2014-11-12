package com.hbase.hbase.performancetest;

import org.apache.log4j.Logger;

import com.hbase.hbase.util.ShowRecord;

public class ResearchMain {
	
	static private Logger log = Logger.getLogger(ResearchMain.class.getName());

	public static void main(String[] args) {
		
		//查询指定条件的数据  消耗时间
		long all = System.currentTimeMillis();
		//ShowRecord.showOneRecordByRowKey_cloumn("newstable1", "327140291-2013-12-21 12:01:00.0","news");  //用时2.75s
		//ShowRecord.showOneRecordByRowKey("newstable", "396026347-2014-04-02 09:16:04.0");  //用时0.794s
		ShowRecord.showRecordsByBetweenRowKey("newstable", "142501397-2013-05-14 11:39:17.0", "150467186-2013-02-22 23:59:00.0");   //2.8s-没有setCaching
		long end = System.currentTimeMillis();
		log.info("search time using " + (end - all)*1.0/1000+"s");
	}

}
