package com.fonsview.cassandra;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.UUID;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;

public class DoInsert implements Runnable{
	
	public static long totalnum=0;//不同线程加起来插入数据总数
	
	private String keyspace;
	private String viewlogDir;
	private long maxCount;
	
	private long count=0;
	private long beginTime;
	
	private long outputmum=10000;
	
	private Session session;
	
	public static long threadCount = 0;
	
	public DoInsert(Session session) {
		this.session = session;
	}

	public DoInsert(String keyspace, String viewlogDir, long maxCount,Session session) {
		this.keyspace = keyspace;
		this.viewlogDir = viewlogDir;
		this.maxCount = maxCount;
		this.session = session;
	}

	public Session getSession() {
		return session;
	}

	public void setSession(Session session) {
		this.session = session;
	}

	public long getMaxCount() {
		return maxCount;
	}

	public void setMaxCount(long maxCount) {
		this.maxCount = maxCount;
	}

	public String getViewlogDir() {
		return viewlogDir;
	}

	public void setViewlogDir(String viewlogDir) {
		this.viewlogDir = viewlogDir;
	}

	public String getKeyspace() {
		return keyspace;
	}

	public void setKeyspace(String keyspace) {
		this.keyspace = keyspace;
	}

	public void createSchema() {
		 try {
			 getSession().execute("CREATE KEYSPACE "+getKeyspace()+" WITH REPLICATION = {'class' : 'SimpleStrategy','replication_factor': 3};");
			String sql = "CREATE TABLE "+getKeyspace()+".viewlog(id uuid PRIMARY KEY,beginTime timestamp,contentID varchar,contentName varchar," +
					"duration varchar, endTime timestamp,serviceType varchar,shiftDuration varchar,userId varchar,userName varchar);";
			getSession().execute(sql);
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}
	
	public void loadData() {
		//获取viewlog文件路径filePath下的文件
		List<File> files = getNeedParseFiles(getViewlogDir());
		if(files == null || files.size() <=0){
			System.out.println("on the localhost the filepath:"+viewlogDir+"	has no viewlog file to parse.");
			return ;
		}
		beginTime = System.currentTimeMillis();
		//读取viewlog
		while (parseFiles(files)) {
			continue;
		}
	}
	
	private boolean parseFiles(List<File> files) {
		for (File file : files) {
			if(file.isDirectory()){
				System.out.println("the file is Directory:"+file.getName()+",no need to parse");
				continue;
			}
			if(!processViewLogFile(file,"|")){
				return false;
			}
		}
		if (count>maxCount || totalnum>maxCount) {
			return false;
		}
		return true;
	}
	
	private boolean processViewLogFile(File file,String separetor) {
		BufferedReader br = null;
		try {
			br = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
			String line=null;
			while((line = br.readLine()) != null){
				if(line == null || line.trim().equals("")){
					continue;
				}
				String[] fields=line.split(separetor,-1);
				if (count>=maxCount) {
					return false;
				}
				count ++;
				if (count%outputmum==0) {
					long endTime = System.currentTimeMillis();
					 System.out.println(String.format("%-30s\t%-20s\t%-20s", Thread.currentThread().getName(),
							 count,  endTime-beginTime+" ms"));
				}
				if(!insert(fields)){
					return false;
				}
				if (count>getMaxCount()) {
					return false;
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null) {
					br.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			} 
		}
		
		return true;
	}

	private boolean insert(String[] fields) {
		if (fields==null || fields.length<10) {
			return false;
		}
		String userId = fields[0].trim();
		String userName = fields[1].trim();
		String begintime = fields[2].trim();
		String endtime = fields[3].trim();
		String serviceType = fields[4].trim();
		String contentID = fields[5].trim();
		String contentName = fields[6].trim();
		String shiftDuration = fields[7].trim();
		String duration = fields[8].trim();
	    String sql = "INSERT INTO  "+getKeyspace()+".viewlog (id,beginTime,contentID,contentName,duration,endTime,serviceType,shiftDuration,userId,userName) " +
	    		"VALUES (?,?,?,?,?,?,?,?,?,?);";
	    try {
			PreparedStatement statement = getSession().prepare(sql);
		    BoundStatement boundStatement = new BoundStatement(statement);
		    getSession().execute(boundStatement.bind(
			     UUID.randomUUID(),
			     parseDate(begintime),
			     contentID,
			     contentName,
			     duration,
			     parseDate(endtime),
			     serviceType,
			     shiftDuration,
			     userId,
			     userName
			     ) );
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	public java.util.Date parseDate(String date){
		String pattern = "yyyy-MM-dd HH:mm:ss";
		Date d = null;
		try {
			d = parseDate(date, pattern);
		} catch (ParseException e) {
			return new Date();
		}
		
		return d;
	}
	
	public java.util.Date parseDate(String date, String pattern)
			throws ParseException
	{
		SimpleDateFormat sdf = new SimpleDateFormat(pattern);
		return sdf.parse(date);
	}
	
	private List<File> getNeedParseFiles(String localPath) {
		File[] files = new File(localPath).listFiles(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				final String re="^Stat_media_view\\.log.*$";
				if(name.matches(re)){
					if (name.endsWith(".tmp") || name.endsWith(".reading") || name.endsWith(".error")) {
						return false;
					}
					return true;
				}else{
					return false;
				}
			}
		});
		return Arrays.asList(files);
	}
	
	@Override
	public void run() {
		loadData();
		totalnum = totalnum+count;
		threadCount--;
		getSession().shutdown();
	}
}
