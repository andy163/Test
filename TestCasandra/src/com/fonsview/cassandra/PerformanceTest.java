package com.fonsview.cassandra;

import java.text.SimpleDateFormat;
import java.util.Date;

import com.datastax.driver.core.Session;

public class PerformanceTest{

	private static String host = "127.0.0.1";
	private static String keyspace = "testdb";
	private static String viewlogDir = "D:\\SecureCRT\\download\\stat_viewlog";
	private static long maxCount = 100000;
	private static long maxThreadCount = 10;
	
	
	public static void main(String[] args) {
		host = args[0];
		keyspace = args[1];
		viewlogDir = args[2];
		maxCount = Long.valueOf(args[3]);
		maxThreadCount = Long.valueOf(args[4]);
		System.out.println("connect host :"+host);
		System.out.println("keyspace :"+keyspace);
		System.out.println("viewlogDir :"+viewlogDir);
		System.out.println("maxCount :"+maxCount);
		System.out.println("maxThreadCount :"+maxThreadCount);
		
		//连接服务器
		Session session = Util.connect(host);
		
		//创建keyspace
		DoInsert insert = new DoInsert(session);
		insert.setKeyspace(keyspace);
		insert.createSchema();
	    
		System.out.println(String.format("%-30s\t%-20s\t%-20s\n%s", "Thread Name", "viewlog count", "spended time",
				"-------------------------------+-----------------------+--------------------"));
		String begintime = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS").format(new Date());
		long l1 = System.currentTimeMillis();
		for (int i = 0; i < maxThreadCount; i++) {
			Session s = Util.connect(host);
			DoInsert client = new DoInsert(keyspace,viewlogDir,maxCount,s);
			//多线程插入viewlog
			DoInsert.threadCount++;
			new Thread(client,"thread--"+i).start();
		}
		while (true) {
			if (DoInsert.threadCount==0) {
				long l2 = System.currentTimeMillis();
				System.out.println("begin time："+begintime);
				System.out.println("end time："+new SimpleDateFormat("yyyy-MM-dd HH:mm:ss SSS").format(new Date()));
				System.out.println("total insert："+DoInsert.totalnum+" records,"+"spend time："+(l2-l1)+" ms,"+""+DoInsert.totalnum*1000/(l2-l1)+" records/s");
				Util.cluster.shutdown();
				break;
			}
		}
	}

}
