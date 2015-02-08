package textcensoring;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.io.Text;

public final class TextCensoring extends UDF{
	public Text evaluate(Text input){
		if(input == null) return null;
		ArrayList<String> bannedWords = new ArrayList<String>();
		try {
			URL sourLink = new URL("https://s3.amazonaws.com/F14CloudTwitterData/banned.txt");
			URLConnection conn = sourLink.openConnection();

            // open the stream and put it into BufferedReader
            BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));

            String inputLine;
            while ((inputLine = in.readLine()) != null) {
            	bannedWords.add(inputLine.trim());
            }
            in.close();
			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		for (int i = 0; i < bannedWords.size(); i++){
			String target = rot13(bannedWords.get(i));
			//System.out.print(target+'\n');
			int wordSize = target.length();
			char[] asteriskArray = new char[wordSize-2];
			for(int j=0; j<wordSize-2; j++){
				asteriskArray[j]='*';
			}
			String asteriskRepl = new String(asteriskArray);
			//System.out.print(asteriskRepl+'\n');
			Pattern p = Pattern.compile("(\\W)("+target+")(\\W)", Pattern.CASE_INSENSITIVE| Pattern.UNICODE_CASE);
			StringBuffer output = new StringBuffer();
			Matcher m = p.matcher(input.toString());
		    while (m.find()) {
		    	String pattern = "(\\w)(\\w+)(\\w)";
		    	m.appendReplacement(output, m.group(1)+m.group(2).replaceAll(pattern, "$1"+asteriskRepl+"$3")+m.group(3));
		    }
		    m.appendTail(output);
		    input = new Text(output.toString());
		}
		
	    return input;
	}
	private static String rot13(String input) {
		   StringBuilder sb = new StringBuilder();
		   for (int i = 0; i < input.length(); i++) {
		       char c = input.charAt(i);
		       if       (c >= 'a' && c <= 'm') c += 13;
		       else if  (c >= 'A' && c <= 'M') c += 13;
		       else if  (c >= 'n' && c <= 'z') c -= 13;
		       else if  (c >= 'N' && c <= 'Z') c -= 13;
		       sb.append(c);
		   }
		   return sb.toString();
	}

}
