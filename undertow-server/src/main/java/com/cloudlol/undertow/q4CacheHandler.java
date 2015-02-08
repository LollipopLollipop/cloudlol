package com.cloudlol.undertow;


import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;

import java.io.IOException;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Date;
import java.util.HashMap;
import java.util.Scanner;
import java.util.Arrays;

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
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.HConnection;
final class q4CacheHandler implements HttpHandler{
	private final String connectString = "cloudlol,1692-2855-0258,9301-4386-3167\n";
	//private HTablePool hbasepool;
	private HConnection connection;
	public final static byte[] table1 = Bytes.toBytes("q4"), //
	       family1 = Bytes.toBytes("cf"), //	     
	       qualifier1 = Bytes.toBytes("content"),
	       changeline = Bytes.toBytes("\n");
	String myDirectoryPath = "/home/ubuntu/q4data";
	HashMap<String,Integer> locationMap;
	HashMap<Integer,Integer> dateMap;
	int [][]valuePosStart;
	int [][]valuePosEnd;
	String[] valueList;
	int[] valuePosList;
	int tweetCount = 34037330;
	long[] tweetList = new long[tweetCount];
	public q4CacheHandler(HConnection connection) throws IOException,FileNotFoundException{
		//this.hbasepool = hbasepool;
		this.connection = connection;

		//build cache
		Runtime runtime = Runtime.getRuntime();
		long memory = runtime.totalMemory() - runtime.freeMemory();
		long MEGABYTE = 1024L * 1024L;
		System.out.println("Used memory is megabytes: " + memory / MEGABYTE);


		locationMap = new HashMap<String,Integer>();
		dateMap = new HashMap<Integer,Integer>();

		/*
		   ArrayList<String> contentList = new ArrayList<String>();
		   HashMap<Integer,Integer> positionList = new HashMap<Integer,Integer>();
		 */

		tweetList = new long[tweetCount];
		//ArrayList<Long> tweetList = new ArrayList<Long>();

		File dir = new File(myDirectoryPath);
		File[] directoryListing = dir.listFiles();
		Date date1 = new Date();
		int locationIndex = 0;
		int dateIndex = 0;

		if(directoryListing != null)
		{
			int lineCount = 0;
			//build hashmap for location and date
			for (File child : directoryListing) 
			{
				Scanner scan = new Scanner(child);
				String part1,content;
				String rank;
				scan.useDelimiter("[\t\n]");
				while(scan.hasNext())
				{
					lineCount ++;
					part1 = scan.next();
					content = scan.next();
					rank = scan.next();
					int rankValue = Integer.valueOf(rank);
					String date = part1.substring(0, 10).replaceAll("-", "");
					int dateValue = Integer.valueOf(date);
					if(!dateMap.containsKey(dateValue))
					{
						dateMap.put(dateValue, dateIndex);
						dateIndex ++;
					}
					String location = part1.substring(11);
					if(!locationMap.containsKey(location))
					{
						locationMap.put(location, locationIndex);
						locationIndex ++;
					}
				}
				scan.close();
			}
			int dateSize = dateMap.size();
			int locationSize = locationMap.size();

			valuePosStart = new int[dateSize][locationSize];
			valuePosEnd = new int[dateSize][locationSize];
			valueList = new String[lineCount];
			valuePosList = new int[lineCount];

			int fileIndex = 0;
			int lineIndex = 0;
			int tweetIndex = 0;

			long tempTweet = 0;
			for (File child : directoryListing) 
			{
				fileIndex ++;
				System.out.println("Processing " + fileIndex);
				Scanner scan = new Scanner(child);
				String part1,content;
				String rank;
				scan.useDelimiter("[\t\n]");
				while(scan.hasNext())
				{
					part1 = scan.next();
					content = scan.next();
					rank = scan.next();
					int rankValue = Integer.valueOf(rank);
					
					String date = part1.substring(0, 10).replaceAll("-", "");
					int dateValue = dateMap.get(Integer.valueOf(date));								
				
					String location = part1.substring(11);
					int locationValue = locationMap.get(location);
					valuePosEnd[dateValue][locationValue] = lineIndex;
					if(rankValue == 1)
					{						
						valuePosStart[dateValue][locationValue] = lineIndex;					
					}			
					// split value
					int index1 = content.indexOf(':');
					String tempValue = content.substring(0, index1);
					valueList[lineIndex] = tempValue;
					valuePosList[lineIndex] = tweetIndex;
					// split tweet
					int preIndex = index1 + 1;
					for(int i = index1 + 1;i < content.length();i ++)
					{
						if(content.charAt(i) == ',')
						{
							tempTweet = Long.valueOf(content.substring(preIndex, i));
							//tweetList.add(tempTweet);
							tweetList[tweetIndex] = tempTweet;
							preIndex = i + 1;
							tweetIndex ++;
						}
						if(i == content.length() - 1)
						{

							tempTweet = Long.valueOf(content.substring(preIndex, i + 1));
							//tweetList.add(tempTweet);
							tweetList[tweetIndex] = tempTweet;
							tweetIndex ++;
						}
					}
					lineIndex ++;
				}
				scan.close();
			}
			System.out.println("tweet count" + tweetIndex);
		}
		else
		{
			valuePosStart = new int[1][1];
			valuePosEnd = new int[1][1];
			valueList = new String[1];
			valuePosList = new int[1];
		}
		Date date2 = new Date();
		System.out.println("Time elapsed:" +(date2.getTime() - date1.getTime()) / 1000.0);
		//System.out.println("Content size:" + contentList.size());
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
			StringBuffer resultString = new StringBuffer(this.connectString);
			Deque<String> dates = exchange.getQueryParameters().get("date");
			String date = dates.peekFirst().replaceAll("-", "");
			Deque<String> locations = exchange.getQueryParameters().get("location");
			String location = locations.peekFirst();
			Deque<String> ms = exchange.getQueryParameters().get("m");
			int m = Integer.valueOf(ms.peekFirst());
			Deque<String> ns = exchange.getQueryParameters().get("n");
			int n = Integer.valueOf(ns.peekFirst());

			int dateValue = dateMap.get(Integer.valueOf(date));									
			int locationValue = locationMap.get(location);

			int valueIndex = valuePosStart[dateValue][locationValue];
			int startIndex = valueIndex + m -1;
			int endIndex = valueIndex + n -1;
			endIndex = endIndex < valuePosEnd[dateValue][locationValue] ? endIndex : valuePosEnd[dateValue][locationValue];
			for(int i = startIndex;i <= endIndex ;i ++)
			{
				resultString  = resultString.append(valueList[i]).append(':');
				int tweetStartIndex = valuePosList[i];
				int tweetEndIndex = -1;
				if(i == valuePosList.length - 1)
				{
					tweetEndIndex = tweetList.length - 1;
				}
				else
				{
					tweetEndIndex =  valuePosList[i + 1] -1;
				}

				for(int j = tweetStartIndex; j <= tweetEndIndex ;j ++)
				{
					if(j == tweetEndIndex)
						resultString = resultString.append(tweetList[j]).append('\n');
					else
						resultString = resultString.append(tweetList[j]).append(',');
				}
			}	
			exchange.getResponseHeaders().put(
					Headers.CONTENT_TYPE,
					"text/plain");


			// If a single query then response must be an object
			exchange.getResponseSender().send(resultString.toString());

		}
}

