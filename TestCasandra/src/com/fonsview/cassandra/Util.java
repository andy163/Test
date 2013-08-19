package com.fonsview.cassandra;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class Util {
	
	public static Cluster cluster = null;
	
	public static Session connect(String node) {
		if (cluster==null) {
			cluster = Cluster.builder().addContactPoint(node).build();
		}
		Session session = cluster.connect();
		return session;
	}
	
	public static void close(Session session) {
		session.shutdown();
	}
	
	public static void close(Cluster cluster) {
		cluster.shutdown();
	}
}
