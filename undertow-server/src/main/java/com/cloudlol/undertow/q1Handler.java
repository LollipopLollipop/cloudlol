package com.cloudlol.undertow;

import io.undertow.Undertow;
import io.undertow.server.*;
import io.undertow.util.Headers;

import java.util.Deque;
import java.math.BigInteger;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.util.Date;

class q1Handler implements HttpHandler {
	String connectString = "cloudlol,1692-2855-0258,9301-4386-3167\n";
	BigInteger originValue = new BigInteger("6876766832351765396496377534476050002970857483815262918450355869850085167053394672634315391224052153");
	@Override
	public void handleRequest(HttpServerExchange exchange) throws Exception {
		
		Deque<String> values = exchange.getQueryParameters().get("key");
		String textValue = values.peekFirst();
		BigInteger inputValue = new BigInteger(textValue);
		BigInteger result = inputValue.divide(originValue);
		Date date = new Date();
		String formatDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(date);
		exchange.getResponseHeaders().put(Headers.CONTENT_TYPE,
			"text/plain");
		exchange.getResponseSender().send(result.toString() + "\n" + connectString + formatDate + "\n");
	}
}
