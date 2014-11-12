package com.hbase.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;

public class testParaseColumn {

    
	
	public static void main(String[] args) throws Exception {
		Configuration conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", "10.15.107.63");
		conf.set("hbase.zookeeper.property.clientPort","2183");
		HBaseAdmin admin = new HBaseAdmin(conf);
		HTableDescriptor tableDescriptor = admin.getTableDescriptor(Bytes.toBytes("cf"));
		byte[] name = tableDescriptor.getName();
		System.out.println(new String(name));
		
		byte[][] colkey = KeyValue.parseColumn(Bytes.toBytes("addColumn:time"));
		
		System.out.println("len: " + colkey.length);
		System.out.println(new String(colkey[0]));
		System.out.println(new String(colkey[1]));
		
//		HColumnDescriptor[] columnFamilies = tableDescriptor.getColumnFamilies();
//		for (HColumnDescriptor d : columnFamilies) {
//		    System.out.println(d.getNameAsString());
//	    }
	}
}
