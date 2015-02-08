package com.cloudlol.undertow;


import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;

import java.io.IOException;
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
final class q4HbaseHandler implements HttpHandler{
	private final String connectString = "cloudlol,1692-2855-0258,9301-4386-3167\n";
	//private HTablePool hbasepool;
	private HConnection connection;
	public final static byte[] table1 = Bytes.toBytes("q4"), //
	       family1 = Bytes.toBytes("cf"), //	     
	       qualifier1 = Bytes.toBytes("content"),
	       changeline = Bytes.toBytes("\n");
	public q4HbaseHandler(HConnection connection) throws IOException{
		//this.hbasepool = hbasepool;
		this.connection = connection;
	}

	@Override
		public void handleRequest(HttpServerExchange exchange) throws Exception {
			if (exchange.isInIoThread()) {
				exchange.dispatch(this);
				return;
			}
			String resultString = this.connectString;
			Deque<String> dates = exchange.getQueryParameters().get("date");
			String date = dates.peekFirst();
			Deque<String> locations = exchange.getQueryParameters().get("location");
			String location = locations.peekFirst();
			Deque<String> ms = exchange.getQueryParameters().get("m");
			String m = ms.peekFirst();
			Deque<String> ns = exchange.getQueryParameters().get("n");
			String n = ns.peekFirst();

			//String startRow = date + ";" + location + String.format("%09d",Integer.valueOf(m));
			String startRow = date.concat(";").concat(location).concat( String.format("%09d",Integer.valueOf(m)));
			String endRow = date.concat(";").concat(location).concat( String.format("%09d",Integer.valueOf(n) + 1));

			//String endRow =  date + ";" + location + String.format("%09d",Integer.valueOf(n) + 1);

			Scan scan = new Scan(Bytes.toBytes(startRow),Bytes.toBytes(endRow));
			HTableInterface table = this.connection.getTable("q4");
			ResultScanner scanner = table.getScanner(scan);
			/*
			   Result[] scanResult = scanner.next(10000);
			   int length = 0;
			   for (Result result : scanResult)
			   {
			   length +=  result.getValue(family1,qualifier1).length + changeline.length;
			   }
			 */
			int max = 10000;
			byte[] dest = new byte[max];
			//System.out.println(length);	
			int currentIndex = 0;
			for (Result result : scanner) {
				byte[] temp = result.getValue(family1,qualifier1);
				//System.out.println(Bytes.toString(temp));
				System.arraycopy(temp, 0, dest, currentIndex,temp.length);
				currentIndex += temp.length;
				System.arraycopy(changeline,0,dest,currentIndex,changeline.length);
				currentIndex += changeline.length;
				//resultString += Bytes.toString(result.getValue(Bytes.toBytes("cf"),Bytes.toBytes("content"))) + "\n";
			}
			byte[] resultByte= new byte[currentIndex];
			System.arraycopy(dest,0,resultByte,0,currentIndex);
			resultString = resultString.concat(Bytes.toString(resultByte));
			//System.out.println(resultString);
			
			exchange.getResponseHeaders().put(
					Headers.CONTENT_TYPE,
					"text/plain");


			// If a single query then response must be an object
			exchange.getResponseSender().send(resultString);
			try{
				scanner.close();
				table.close();
			}
			catch(IOException e)
			{
				e.printStackTrace();
			}

		}
}

