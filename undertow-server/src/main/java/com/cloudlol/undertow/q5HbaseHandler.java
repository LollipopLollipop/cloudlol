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
import java.util.Arrays;
import java.util.Date;

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

final class q5HbaseHandler implements HttpHandler{
	private final String connectString = "cloudlol,1692-2855-0258,9301-4386-3167\n";
	//private HTablePool hbasepool;
	private HConnection connection;
	private int Data_Count =  55678811;
	private int[] useridList;
	private int[] p1List; 
	private int[] p2List;
	private int[] p3List;
	public q5HbaseHandler(HConnection connection) throws IOException,FileNotFoundException{
		//this.hbasepool = hbasepool;
		this.connection = connection;
		useridList = new int[Data_Count];
		p1List = new int[Data_Count];
		p2List = new int[Data_Count];
		p3List = new int[Data_Count];
		Runtime runtime = Runtime.getRuntime();
		long memory = runtime.totalMemory() - runtime.freeMemory();
		long MEGABYTE = 1024L * 1024L;
		System.out.println("Used memory is megabytes: " + memory / MEGABYTE);

		Date date1 = new Date();


		Scanner scan = new Scanner(new File("/home/ubuntu/q5Data"));
		long t_userid = 0;
		int p1 = 0,p2 = 0, p3 = 0, p4 = 0;
		int index = 0;
		while(scan.hasNext())
		{
			t_userid = scan.nextLong();
			p1 = scan.nextInt();
			p2 = scan.nextInt();
			p3 = scan.nextInt();
			p4 = scan.nextInt();
			//useridList.add((int)(t_userid + Integer.MIN_VALUE));
			//contentList.add( new buildCacheQ5.Content(p1,p2,p3));
			useridList[index] = (int) (t_userid + Integer.MIN_VALUE);
			p1List[index] = p1;
			p2List[index] = p2;
			p3List[index] = p3;
			index ++;
		}
		scan.close();
		//System.gc();
		System.out.println("Build success");
		memory = runtime.totalMemory() - runtime.freeMemory();
		System.out.println("Used memory is megabytes: " + memory / MEGABYTE);

		Date date2 = new Date();
		System.out.println("Time elapsed:" +(date2.getTime() - date1.getTime()) / 1000.0);
	}

	@Override
		public void handleRequest(HttpServerExchange exchange)  {
			if (exchange.isInIoThread()) {
				exchange.dispatch(this);
				return;
			}
			StringBuffer resultString = new StringBuffer(this.connectString);

			String mValue = exchange.getQueryParameters().get("m").peekFirst();
			String nValue = exchange.getQueryParameters().get("n").peekFirst();
			long m = Long.valueOf(mValue);
			long n = Long.valueOf(nValue);
			
			/*
			   HTableInterface table = this.connection.getTable("q5");
			   Get mGet = new Get(Bytes.toBytes(mValue));
			   Get nGet = new Get(Bytes.toBytes(nValue));
			   List<Get> getList = new ArrayList<Get>();
			   getList.add(mGet);
			   getList.add(nGet);

			   Result[] results = table.get(getList);
			   int index = 0;
			   String m1 = "",m2 = "",m3 = "",mAll = "";
			   String n1 = "",n2 = "",n3 = "",nAll = "";
			   for(Result r : results)
			   {
			   if(index == 0)
			   {
			   m1 = Bytes.toString(r.getValue(Bytes.toBytes("cf"),Bytes.toBytes("point1")));
			   m2 = Bytes.toString(r.getValue(Bytes.toBytes("cf"),Bytes.toBytes("point2")));
			   m3 = Bytes.toString(r.getValue(Bytes.toBytes("cf"),Bytes.toBytes("point3")));
			   mAll = Bytes.toString(r.getValue(Bytes.toBytes("cf"),Bytes.toBytes("pointall")));
			   }
			   else
			   {
			   n1 = Bytes.toString(r.getValue(Bytes.toBytes("cf"),Bytes.toBytes("point1")));
			   n2 = Bytes.toString(r.getValue(Bytes.toBytes("cf"),Bytes.toBytes("point2")));
			   n3 = Bytes.toString(r.getValue(Bytes.toBytes("cf"),Bytes.toBytes("point3")));
			   nAll = Bytes.toString(r.getValue(Bytes.toBytes("cf"),Bytes.toBytes("pointall")));
			   }
			   index ++;
			   }
			   String win1 = "",win2 = "",win3 = "",winAll = "";
			 */
			try{
				int mIndex = Arrays.binarySearch(useridList,(int)(m + Integer.MIN_VALUE));
				int m1 = p1List[mIndex],m2 = p2List[mIndex], m3 = p3List[mIndex];
				int nIndex = Arrays.binarySearch(useridList,(int)(n + Integer.MIN_VALUE));
				int n1 = p1List[nIndex],n2 = p2List[nIndex], n3 = p3List[nIndex];
				String win1 = "",win2 = "",win3 = "",winAll = "";

				if(m1 > n1)
					win1 = mValue;
				else if(m1 == n1)
					win1 = "X";
				else
					win1 = nValue;

				if(m2 > n2)
					win2 = mValue;
				else if(m2 == n2)
					win2 = "X";
				else
					win2 = nValue;

				if(m3 > n3)
					win3 = mValue;
				else if(m3 == n3)
					win3 = "X";
				else
					win3 = nValue;
				int mAll = m1 + m2 + m3;
				int nAll = n1 + n2 + n3;
				if((mAll) > (nAll))
					winAll = mValue;
				else if((mAll) == (nAll))
					winAll = "X";
				else
					winAll = nValue;

				resultString = resultString.append(mValue)
					.append("\t")
					.append(nValue)
					.append("\t")
					.append("WINNER")
					.append("\n")
					.append(m1)
					.append("\t")
					.append(n1)
					.append("\t")
					.append(win1)
					.append("\n")
					.append(m2)
					.append("\t")
					.append(n2)
					.append("\t")
					.append(win2)
					.append("\n")
					.append(m3)
					.append("\t")
					.append(n3)
					.append("\t")
					.append(win3)
					.append("\n")
					.append(mAll)
					.append("\t")
					.append(nAll)
					.append("\t")
					.append(winAll)
					.append("\n");

				//System.out.println(resultString);
				exchange.getResponseHeaders().put(
						Headers.CONTENT_TYPE,
						"text/plain");
				// If a single query then response must be an object
				exchange.getResponseSender().send(resultString.toString());
			}
			catch(Exception e)
			{
				e.printStackTrace();
			}

			/*
			   try{
			   table.close();
			   }
			   catch(IOException e)
			   {
			   e.printStackTrace();
			   }
			 */

		}
}


