package com.cloudlol.undertow;


import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;

import java.io.IOException;
import java.util.Arrays;

import java.util.Deque;
import java.util.List;
import java.util.ArrayList;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HConnection;

final class q6HbaseHandler implements HttpHandler{
	private final String connectString = "cloudlol,1692-2855-0258,9301-4386-3167\n";
	//private HTablePool hbasepool;
	private HConnection connection;
	private ArrayList<Long> useridList;
	private ArrayList<Integer> countList; 
	public q6HbaseHandler(HConnection connection) throws IOException,FileNotFoundException{
		//this.hbasepool = hbasepool;
		this.connection = connection;
		this.useridList = new ArrayList<Long>();
		this.countList = new ArrayList<Integer>();
		Runtime runtime = Runtime.getRuntime();
		long memory = runtime.totalMemory() - runtime.freeMemory();
		long MEGABYTE = 1024L * 1024L;
		System.out.println("Used memory is megabytes: " + memory / MEGABYTE);

		Scanner scan = new Scanner(new File("/home/ubuntu/q6Data"));
		long t_userid = 0;
		int t_count = 0;
		while(scan.hasNext())
		{
			t_userid = scan.nextLong();
			t_count = scan.nextInt();
			this.useridList.add(t_userid);
			this.countList.add(t_count);
		}
		scan.close();
		System.out.println("Build success");
		
		memory = runtime.totalMemory() - runtime.freeMemory();
		System.out.println("Used memory is megabytes: " + memory / MEGABYTE);
	}

	@Override
		public void handleRequest(HttpServerExchange exchange) throws Exception {
			if (exchange.isInIoThread()) {
				exchange.dispatch(this);
				return;
			}
			String resultString = this.connectString;

			String mValue = exchange.getQueryParameters().get("m").peekFirst();
			String nValue = exchange.getQueryParameters().get("n").peekFirst();

			long m = Long.valueOf(mValue);
			long n = Long.valueOf(nValue);
			int nIndex = Collections.binarySearch(this.useridList,n);
			if(nIndex < 0) nIndex = -(nIndex + 1) - 1;
			int nCount = 0;
			if(nIndex >= 0) nCount = this.countList.get(nIndex);
			int mIndex = Collections.binarySearch(this.useridList,m-1);
			if(mIndex < 0) mIndex = -(mIndex + 1) - 1;
			int mCount = 0;
			if(mIndex >= 0) mCount = this.countList.get(mIndex);
			exchange.getResponseHeaders().put(
					Headers.CONTENT_TYPE,
					"text/plain");
			// If a single query then response must be an object
			exchange.getResponseSender().send(resultString.concat(new Integer(nCount - mCount).toString()).concat("\n"));



		}
}


