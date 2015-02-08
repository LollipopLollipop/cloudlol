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
		int inputLength = input.toString().length();
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
			int wordSize = target.length();
			ArrayList<Integer> positions = new ArrayList();
		    Pattern p = Pattern.compile(target);  // insert your pattern here
		    Matcher m = p.matcher(input.toString().toLowerCase());
		    while (m.find()) {
		    	if((m.start() > inputLength-wordSize))
		    		break;
		    	if((m.start() < inputLength-wordSize) && isAlphaNumeric(input.toString().charAt(m.start()+wordSize)))
		    		break;
		    	if((m.start()!=0) && (isAlphaNumeric(input.toString().charAt(m.start()-1))))
		    		break;
		    	positions.add(m.start());
		    }
		    StringBuilder myString = new StringBuilder(input.toString());
		    for (int j = 0; j < positions.size(); j++){
		    	for (int k = 0; k < wordSize-2; k++)
		    		myString.setCharAt(positions.get(j)+k+1, '*');
		    }
		    input = new Text(String.valueOf(myString));
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
	private static boolean isAlphaNumeric(char c){
	        if((c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <='z')){
	            return true;
	        }
	        return false;   
	}

}
