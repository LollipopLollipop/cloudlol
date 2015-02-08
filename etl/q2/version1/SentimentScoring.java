package sentimentscoring;


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

public final class SentimentScoring extends UDF{
	public int evaluate(final Text input) {
		if(input == null) return 0;
		String inputStr = input.toString();
		int inputLength = inputStr.length();
		int finalScore = 0;
		Scanner scanner;
		
		ArrayList<String> words = new ArrayList<String>();
		int[] scores = new int[2476];
		try {
			URL sourLink = new URL("https://s3.amazonaws.com/F14CloudTwitterData/AFINN.txt");
			URLConnection conn = sourLink.openConnection();

            // open the stream and put it into BufferedReader
            BufferedReader in = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            int i = 0;
            String inputLine;
            while ((inputLine = in.readLine()) != null) {
            	words.add(inputLine.trim().split("\t")[0]);
            	scores[i] = Integer.parseInt(inputLine.trim().split("\t")[1]);
            	i++;
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
		
		for (int i = 0; i < words.size(); i++){
			//System.out.print(i+" " +words.get(i));
			String target = words.get(i);
			int wordSize = target.length();
			ArrayList<Integer> positions = new ArrayList();
		    Pattern p = Pattern.compile(target);  // insert your pattern here
		    Matcher m = p.matcher(inputStr.toLowerCase());
		    while (m.find()) {
		    	if((m.start() > inputLength-wordSize))
		    		break;
		    	if((m.start() < inputLength-wordSize) && isAlphaNumeric(inputStr.charAt(m.start()+wordSize)))
		    		break;
		    	if((m.start()!=0) && (isAlphaNumeric(inputStr.charAt(m.start()-1))))
		    		break;
		    	positions.add(m.start());
		    }
		    //System.out.print(scores[i]+"\n");
		    finalScore = finalScore + positions.size()*scores[i];
		}
		
	    return finalScore;
	}
	private static boolean isAlphaNumeric(char c){
        if((c >= '0' && c <= '9') || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <='z')){
            return true;
        }
        return false;   
}
}
