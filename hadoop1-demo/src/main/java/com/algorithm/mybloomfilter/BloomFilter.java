package com.algorithm.mybloomfilter;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.BitSet;
import java.util.Random;

import org.apache.hadoop.io.Writable;

//来源：http://mazd1002.blog.163.com/blog/static/66574965201110221129696/
//或者 hadoop实战课本
public class BloomFilter<E> implements Writable{
	
	    private BitSet bf;
	    
	    private int bitArraySize = 100000000; //设定set集合的大小
	    private int numHashFunc = 6;  //设定传说中的k值
	    
	    public BloomFilter(){
	        bf= new BitSet(bitArraySize);
	    }
	    
	    public void add(E obj){
	        //添加一个obj，会将obj在BitSet中所对应的所有位置都设置为相应的k值
	        //具体k值设定在getHashIndexes()方法中实现
	        int [] indexes = getHashIndexes(obj);
	        for(int index:indexes){
	            bf.set(index); //设置为index到indexes,此处为什么不设置为1呢？
	            //bf.set(1);    为什么不这样子呢？ 2011-12-10
	        }
	    }
	    
	    public boolean contains(E obj)
	    {
	        int [] indexes = getHashIndexes(obj);
	        for(int index:indexes)
	        {   
	            //只要有一个为false，那么全部都为false
	            if(bf.get(index) == false)
	            {
	                return false;
	            }
	        }
	        return true;
	    }

	    //我感觉能把这个函数写出来的才算是牛人
	    protected int [] getHashIndexes(E obj){
	        int [] indexes = new int[numHashFunc]; //根据k值来设定indexes
	        
	        long seed = 0; //作用是什么？ 2011-12-10  在后面的Random中使用到.这个seed是Random中的种子数
	        byte[] digest;
	        try{
	            MessageDigest md = MessageDigest.getInstance("MD5"); //获取MD5值？
	            md.update(obj.toString().getBytes()); //把obj转换成字节
	            digest = md.digest();
	            for(int i=0; i< numHashFunc; i++){
	                seed =seed|((long)digest[i]&0xFF)<<(8*i);   //最最复杂，最最智慧的一部分，其实他仿造了Random中seed生成方法
	            }
	        }catch(NoSuchAlgorithmException e){            
	        }
	        Random gen = new Random(seed);//相同种子数的Random对象，相同次数生成的随机数字是完全相同的
	        for(int i=0; i<numHashFunc; i++){
	            indexes[i] = gen.nextInt(bitArraySize);
	        }
	        return indexes;
	    }
	    
	    public void union(BloomFilter<E> other){
	        bf.or(other.bf);
	    }

	    @Override
	    public void readFields(DataInput in) throws IOException {
	        int byteArraySize = (int)(bitArraySize/8);
	        byte[] byteArray = new byte[byteArraySize];
	        
	        in.readFully(byteArray);
	        
	        for(int i=0; i<byteArraySize; i++){
	            byte nextByte = byteArray[i];
	            for(int j=0; j<8; j++){
	                if(((int)nextByte & (1<<j))!=0){
	                    bf.set(8*i+j);
	                }
	            }
	        }
	    }

	    
	    @Override
	    public void write(DataOutput out) throws IOException {
	        int byteArraySize = (int)(bitArraySize/8);
	        byte[] byteArray = new byte[byteArraySize];
	        for(int i=0; i<byteArraySize; i++){
	            byte nextElement =0;
	            for(int j=0; j<8; j++){
	                if(bf.get(8*i+j)){
	                    nextElement|=1<<j;
	                }
	            }
	            byteArray[i] = nextElement;
	        }
	        out.write(byteArray);
	    }
}
