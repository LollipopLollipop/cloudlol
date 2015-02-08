package com.cloudlol.undertow;

import com.fasterxml.jackson.databind.ObjectMapper;

import io.undertow.server.HttpHandler;
import io.undertow.server.HttpServerExchange;
import io.undertow.util.Headers;

import javax.sql.DataSource;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Objects;
import java.util.Deque;
import java.sql.Statement;


/**
 * Handles the single- and multiple-query database tests using a SQL database.
 */
final class q4Handler implements HttpHandler {
	private final ObjectMapper objectMapper;
	private final DataSource database;
	private final boolean multiple;
	private final String connectString = "cloudlol,1692-2855-0258,9301-4386-3167\n";

	public q4Handler(ObjectMapper objectMapper,DataSource database, boolean multiple) {
		this.objectMapper = Objects.requireNonNull(objectMapper);
		this.database = Objects.requireNonNull(database);
		this.multiple = multiple;
	}

	@Override
		public void handleRequest(HttpServerExchange exchange) throws Exception {
			if (exchange.isInIoThread()) {
				exchange.dispatch(this);
				return;
			}
			String resultString = this.connectString;
			Deque<String> values1 = exchange.getQueryParameters().get("date");
			String date = values1.peekFirst();
			Deque<String> values2 = exchange.getQueryParameters().get("location");
			String location = values2.peekFirst();
			Deque<String> values3 = exchange.getQueryParameters().get("m");
			String m = values3.peekFirst();
			Deque<String> values4 = exchange.getQueryParameters().get("n");
			String n = values4.peekFirst();

			Connection connection = database.getConnection();
			Statement statement = connection.createStatement();
			String query = "select content from q4 where query='" + date + ";"+location + "' and rank>=" + m + " and rank<=" + n + " order by rank";
			ResultSet resultSet = statement.executeQuery(query);
					

			while(resultSet.next())
			{
				String content = resultSet.getString("content");
				resultString += (content + "\n");
			} 


			exchange.getResponseHeaders().put(
					Headers.CONTENT_TYPE,
					"text/plain");


			// If a single query then response must be an object
			exchange.getResponseSender().send(resultString);

		}
}

