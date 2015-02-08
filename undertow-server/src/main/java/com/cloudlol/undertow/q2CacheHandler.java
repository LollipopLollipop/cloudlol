package com.cloudlol.undertow;


import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;

import java.io.IOException;
import java.util.Arrays;

import java.util.Deque;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
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


final class q2CacheHandler implements HttpHandler{
	private final String connectString = "cloudlol,1692-2855-0258,9301-4386-3167\n";
	//private HTablePool hbasepool;
	private HConnection connection;
	int CONTENT_SIZE = 199266616;
	long[] queryList = new long[CONTENT_SIZE];
	int[] positionList = new int[CONTENT_SIZE];
	public q2CacheHandler(HConnection connection) throws IOException{
		//this.hbasepool = hbasepool;
		this.connection = connection;
		Runtime runtime = Runtime.getRuntime();
		long memory = runtime.totalMemory() - runtime.freeMemory();
		long MEGABYTE = 1024L * 1024L;
		System.out.println("Used memory is megabytes: " + memory / MEGABYTE);

		Date date1 = new Date();
		Scanner scan = new Scanner(new File("/home/ubuntu/q2NewIndex"));
		long query = 0;
		int pos = 0;
		int index = 0;
		while(scan.hasNext())
		{
			query = scan.nextLong();
			pos = scan.nextInt();	
			queryList[index] = query;
			positionList[index] = pos;
			index ++;
			if(index % 100000 == 0) System.out.println(index + "processed");
		}
		scan.close();		
		System.out.println("Build success");
		memory = runtime.totalMemory() - runtime.freeMemory();
		System.out.println("Used memory is megabytes: " + memory / MEGABYTE);

		Date date2 = new Date();
		System.out.println("Time elapsed:" +(date2.getTime() - date1.getTime()) / 1000.0);
	}

	@Override
		public void handleRequest(HttpServerExchange exchange) throws Exception {
			if (exchange.isInIoThread()) {
				exchange.dispatch(this);
				return;
			}
			String resultString = this.connectString;
			Deque<String> values = exchange.getQueryParameters().get("userid");
			String userid = values.peekFirst();
			Deque<String> t_values = exchange.getQueryParameters().get("tweet_time");
			String tweet_time = t_values.peekFirst();

			long useridValue = Long.valueOf(userid);
			SimpleDateFormat ft = new SimpleDateFormat ("YYYY-mm-dd HH:MM:SS");
			Date tempDate = ft.parse(tweet_time);
			long timeValue = (tempDate.getTime() - ft.parse("2013-01-01 12:00:00").getTime()) / 1000;
			long key = useridValue * 1000000000L + timeValue;
			int index = Arrays.binarySearch(queryList,key);
			int pos = positionList[index];
			int fileIndex =  pos / 10000;
			int lineIndex = pos  % 10000;

			BufferedReader scan = new BufferedReader(new FileReader(new File("/home/ubuntu/q2data/" + fileIndex)));
			index = 0;
			for (String line = scan.readLine(); line != null; line = scan.readLine())
			{
				if(index >= lineIndex)
				{
					for(int i = 0;i < line.length();i ++)
					{
						if(line.charAt(i) == '\u0001')
						{
							resultString += line.substring(i + 1);
							break;
						}
					}
					break;
				}
				index ++;
			}
			scan.close();
			exchange.getResponseHeaders().put(
					Headers.CONTENT_TYPE,
					"text/plain");


			// If a single query then response must be an object
			exchange.getResponseSender().send(resultString.replace('\u0002', '\n').replace("\u0009",""));

		}
}

