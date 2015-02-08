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


/**
 * Handles the single- and multiple-query database tests using a SQL database.
 */
final class q3Handler implements HttpHandler {
  private final ObjectMapper objectMapper;
  private final DataSource database;
  private final boolean multiple;
  private final String connectString = "cloudlol,1692-2855-0258,9301-4386-3167\n";

  public q3Handler(ObjectMapper objectMapper, DataSource database, boolean multiple) {
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
    int queries = 1;
    
   
    try (Connection connection = database.getConnection();
         PreparedStatement statement = connection.prepareStatement(
             "SELECT content FROM q3 where userid = ?",
             ResultSet.TYPE_FORWARD_ONLY,
             ResultSet.CONCUR_READ_ONLY)) {
    	Deque<String> values = exchange.getQueryParameters().get("userid");
    String userid = values.peekFirst();
        statement.setNString(1, userid);
        try (ResultSet resultSet = statement.executeQuery()) {
          resultSet.next();
	  String content = resultSet.getString("content");
          resultString += content;
          
        }
    }
    exchange.getResponseHeaders().put(
        Headers.CONTENT_TYPE,
			"text/plain");
    
  
      // If a single query then response must be an object
      exchange.getResponseSender().send(resultString);
   
  }
}
