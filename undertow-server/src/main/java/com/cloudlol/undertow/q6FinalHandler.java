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
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.*;
final class q6FinalHandler implements HttpHandler{
	private final String connectString = "cloudlol,1692-2855-0258,9301-4386-3167\n";
	//private HTablePool hbasepool;
	private HConnection connection;
	private ArrayList<Long> useridList;
	private ArrayList<Integer> countList; 
	private Random generator = new Random(System.currentTimeMillis());
	public final static byte[] table1 = Bytes.toBytes("q6"), //
         family1 = Bytes.toBytes("cf"), //       
         qualifier1 = Bytes.toBytes("content"),
         changeline = Bytes.toBytes("\n");
	public q6FinalHandler(HConnection connection) throws IOException,FileNotFoundException{
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
			//if(generator.nextInt(10) != 0)
			if(true)
			{
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
			else
			{
				String resultString = this.connectString;

				String mValue = exchange.getQueryParameters().get("m").peekFirst();
				String nValue = exchange.getQueryParameters().get("n").peekFirst();
				//System.out.println("m:" + mValue);
				//System.out.println("n:" + nValue);
				HTableInterface table = this.connection.getTable("q6");

				int startValue = 0;
				int endValue = 0;
				Long start = Long.valueOf(mValue) - 1 > 0 ?  Long.valueOf(mValue) - 1  : 0;
				if(start > Integer.MAX_VALUE)
					startValue = Integer.MAX_VALUE;
				else
					startValue = (int)(long)start;
				Long end = Long.valueOf(nValue);
				if(end > Integer.MAX_VALUE)
					endValue = Integer.MAX_VALUE;
				else
					endValue = (int)(long)end;

				String startRow =  String.format("%010d",start);
				String endRow = String.format("%010d",end);

				/*
				   Get mGet = new Get(Bytes.toBytes(mValue));
				   Get nGet = new Get(Bytes.toBytes(nValue));
				   List<Get> getList = new ArrayList<Get>();
				   getList.add(mGet);
				   getList.add(nGet);
				   Result[] results = table.get(getList);
				 */
				int mResult = 0;
				int nResult = 0;

				Scan scan = new Scan(Bytes.toBytes(startRow));
				scan.setMaxResultSize(1);
				scan.setReversed(true);
				ResultScanner scanner = table.getScanner(scan);
				for (Result result : scanner) {
					byte[] temp = result.getValue(family1,qualifier1);
					mResult = Integer.valueOf(Bytes.toString(temp));
					break;
				}

				scan = new Scan(Bytes.toBytes(endRow));
				scan.setMaxResultSize(1);
				scan.setReversed(true);
				scanner = table.getScanner(scan);
				for (Result result : scanner) {
					byte[] temp = result.getValue(family1,qualifier1);
					nResult = Integer.valueOf(Bytes.toString(temp));
					break;
				}      
				exchange.getResponseHeaders().put(
						Headers.CONTENT_TYPE,
						"text/plain");
				exchange.getResponseSender().send(resultString.concat("" + (nResult - mResult) + "\n"));

				try{
					table.close();
				}
				catch(IOException e)
				{
					e.printStackTrace();
				}

			}




		}
}



