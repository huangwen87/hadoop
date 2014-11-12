package com.algorithm.mybloomfilter;




public class TestHadoopBloomFilter {


	public static void main(String[] args) {
		
		String urlTest = "http://www.agrilink.cn/";
        BloomFilter<String> f = new BloomFilter<String>();
        f.add(urlTest);	    
        System.out.println("f.exist(): " + f.contains("aa"));
	}

}
