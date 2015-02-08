package com.cloudlol.undertow;

/**
 * Hello world!
 *
 */
import com.fasterxml.jackson.databind.ObjectMapper;
import io.undertow.Handlers;
import io.undertow.Undertow;
import io.undertow.UndertowOptions;
import io.undertow.util.Headers;
import org.xnio.Options;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
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
//import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.util.*;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HConnection;
/**
 *  * Hello world!
 *   * 
 *    */
public class App {
	public App() throws ClassNotFoundException, IOException, SQLException {
		Configuration config = HBaseConfiguration.create();     
		config.set("hbase.zookeeper.quorum", "ec2-54-174-89-157.compute-1.amazonaws.com");
		config.set("hbase.zookeeper.property.clientPort", "2181");      
		config.set("hbase.master", "ec2-54-174-89-157.compute-1.amazonaws.com:60000");
		//HTablePool hbasepool = new HTablePool(config,1024,PoolMap.PoolType.ThreadLocal);
		HConnection connection = HConnectionManager.createConnection(config);
		Class.forName("com.mysql.jdbc.Driver");
		final ObjectMapper objectMapper = new ObjectMapper();
		final DataSource mysql = Helper.newDataSource(
				"jdbc:mysql://54.173.88.197:3306/15619project?jdbcCompliantTruncation=false&elideSetAutoCommits=true&useLocalSessionState=true&cachePrepStmts=true&cacheCallableStmts=true&alwaysSendSetIsolation=false&prepStmtCacheSize=4096&cacheServerConfiguration=true&prepStmtCacheSqlLimit=2048&zeroDateTimeBehavior=convertToNull&traceProtocol=false&useUnbufferedInput=false&useReadAheadInput=false&maintainTimeStats=false&useServerPrepStmts&cacheRSMetadata=true",
				"user",
				"password");
		//
		// The world cache is primed at startup with all values.  It doesn't
		// matter which database backs it; they all contain the same information
		// and the CacheLoader.load implementation below is never invoked.
		//

		Undertow.builder()
			.addHttpListener(
					80,
					"172.31.32.91")
			.setBufferSize(1024 * 16)
			.setIoThreads(Runtime.getRuntime().availableProcessors() * 2) //this seems slightly faster in some configurations
			.setSocketOption(Options.BACKLOG, 10000)
			.setServerOption(UndertowOptions.ALWAYS_SET_KEEP_ALIVE, false) //don't send a keep-alive header for HTTP/1.1 requests, as it is not required
			.setServerOption(UndertowOptions.ALWAYS_SET_DATE, true)
			.setHandler(Handlers.header(Handlers.path()
						.addPrefixPath("/q1",
							new q1Handler())
						//.addPrefixPath("/q2",
						//		new q2CacheHandler(connection)),
						.addPrefixPath("/q3",
							new q3FinalHandler(connection))
						.addPrefixPath("/q4",
							new q4FinalHandler(connection))
						.addPrefixPath("/q5",
					   			new q5FinalHandler(connection))
						.addPrefixPath("/q6",
							new q6FinalHandler(connection)),
						Headers.SERVER_STRING, "U-tow"))
			.setWorkerThreads(512)
			.build()
			.start();
	}
	public static void main(final String[] args) throws IOException,SQLException,ClassNotFoundException {
		new App();
	}
}


