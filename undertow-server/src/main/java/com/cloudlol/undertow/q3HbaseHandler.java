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
import org.apache.hadoop.hbase.client.HConnection;

final class q3HbaseHandler implements HttpHandler{
	private final String connectString = "cloudlol,1692-2855-0258,9301-4386-3167\n";
	//private HTablePool hbasepool;
	private HConnection connection;
	public q3HbaseHandler(HConnection connection) throws IOException{
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
			Deque<String> values = exchange.getQueryParameters().get("userid");
			String userid = values.peekFirst();
			HTableInterface table = this.connection.getTable("q3");
			Get g = new Get(Bytes.toBytes(userid));
			Result r = table.get(g);
			String Value = Bytes.toString(r.getValue(Bytes.toBytes("cf"),Bytes.toBytes("content")));
			resultString += Value.replace('\u0001', '\n');
			try{
				table.close();
			}
			catch(IOException e)
			{
				e.printStackTrace();
			}

			exchange.getResponseHeaders().put(
					Headers.CONTENT_TYPE,
					"text/plain");


			// If a single query then response must be an object
			exchange.getResponseSender().send(resultString);


		}
}

