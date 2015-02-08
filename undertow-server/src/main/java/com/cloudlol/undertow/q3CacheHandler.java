package com.cloudlol.undertow;


import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;

import java.io.File;
import java.io.IOException;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Date;
import java.util.Scanner;
import java.util.Deque;

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

final class q3CacheHandler implements HttpHandler{
	private final String connectString = "cloudlol,1692-2855-0258,9301-4386-3167\n";
	//private HTablePool hbasepool;
	private HConnection connection;
	private int[] useridList;
	private int[] startList;
	private int[] endList;

	private int[] retweetList;
	private boolean[] retweetTypeList;

	private static int userCount = 13888216;
	private static int retweetCount = 330942754;

	String myDirectoryPath = "/home/ubuntu/q3data";
	public q3CacheHandler(HConnection connection) throws IOException,FileNotFoundException{
		//this.hbasepool = hbasepool;
		this.connection = connection;
		Runtime runtime = Runtime.getRuntime();
		long memory = runtime.totalMemory() - runtime.freeMemory();
		long MEGABYTE = 1024L * 1024L;
		System.out.println("Used memory is megabytes: " + memory / MEGABYTE);

		//this.useridList = new HashMap<Integer,Position>();
		this.useridList = new int[userCount];
		this.startList = new int[userCount];
		this.endList = new int[userCount];

		this.retweetList = new int[retweetCount];
		this.retweetTypeList = new boolean[retweetCount];

		File dir = new File(myDirectoryPath);
		File[] directoryListing = dir.listFiles();

		Date date1 = new Date();



		if(directoryListing != null)
		{			
			// read useridlist
			int lineIndex  = 0;
			for (File child : directoryListing) 
			{
				Scanner scan = new Scanner(child);
				String userid;
				//String content;
				scan.useDelimiter("[\t\n]");
				while(scan.hasNext())
				{
					userid = scan.next();
					scan.next();
					//int tempUserId = (int)(Long.valueOf(userid) + Integer.MIN_VALUE);
					this.useridList[lineIndex] = (int)(Long.valueOf(userid) + Integer.MIN_VALUE);
					lineIndex ++;
				}
				scan.close();
			}
			lineIndex = 0;
			// sort useridlist
			Arrays.sort(useridList);
			// retweet
			int retweetIndex = 0;
			for (File child : directoryListing) 
			{
				Scanner scan = new Scanner(child);
				String userid;
				String content;
				scan.useDelimiter("[\t\n]");
				while(scan.hasNext())
				{
					userid = scan.next();
					content = scan.next();
					int tempUserId = (int)(Long.valueOf(userid) + Integer.MIN_VALUE);
					int tempStart = retweetIndex;
					int tempEnd = retweetIndex;
					int userIndex = Arrays.binarySearch(useridList,tempUserId);
					int preIndex = 0;
					for(int i = 0;i < content.length();i ++)
					{
						if(content.charAt(i) == '\u0001')
						{
							if(content.charAt(preIndex) == '(')
							{
								this.retweetList[retweetIndex] = (int)(Long.valueOf(content.substring(preIndex + 1, i - 1)) + Integer.MIN_VALUE);
								this.retweetTypeList[retweetIndex] = true;
							}
							else
							{
								this.retweetList[retweetIndex] = (int)(Long.valueOf(content.substring(preIndex, i)) + Integer.MIN_VALUE);
								this.retweetTypeList[retweetIndex] = false;
							}
							tempEnd = retweetIndex;
							retweetIndex ++;
							preIndex = i + 1;
						}
					}
					this.startList[userIndex] = tempStart;
					this.endList[userIndex] = tempEnd;
				}
				scan.close();
			}
		}
		Date date2 = new Date();
		System.out.println("Time elapsed:" +(date2.getTime() - date1.getTime()) / 1000.0);
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
			Deque<String> values = exchange.getQueryParameters().get("userid");
			int userid = (int)(Long.valueOf(values.peekFirst()) + Integer.MIN_VALUE);

			StringBuffer resultString = new StringBuffer(this.connectString);
			//String resultString = "";
			int index = Arrays.binarySearch(useridList,userid);
			for(int i = this.startList[index]; i <= this.endList[index];i ++)
			{
				long tempValue = (long)retweetList[i] - Integer.MIN_VALUE;
				if(retweetTypeList[i] == true)
				{
					resultString = resultString.append('(').append(tempValue).append(")\n"); 
				}
				else
				{
					resultString = resultString.append(tempValue).append('\n'); 
				}
			}
			exchange.getResponseHeaders().put(
					Headers.CONTENT_TYPE,
					"text/plain");


			// If a single query then response must be an object
			exchange.getResponseSender().send(resultString.toString());


		}
}

