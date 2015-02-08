package com.cloudlol.undertow;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Date;
import java.util.HashMap;
import java.util.Scanner;

public class buildCacheQ4 {
	String myDirectoryPath = "/home/ubuntu/q4data";
	HashMap<String,Integer> locationMap;
	HashMap<Integer,Integer> dateMap;
	int [][]valuePos;
	String[] valueList;
	int[] valuePosList;
	int tweetCount = 34037330;
	long[] tweetList = new long[tweetCount];
	
	public buildCacheQ4() throws FileNotFoundException
	{
		Runtime runtime = Runtime.getRuntime();
		long memory = runtime.totalMemory() - runtime.freeMemory();
		long MEGABYTE = 1024L * 1024L;
		System.out.println("Used memory is megabytes: " + memory / MEGABYTE);
		
		
		locationMap = new HashMap<String,Integer>();
		dateMap = new HashMap<Integer,Integer>();

		/*
		ArrayList<String> contentList = new ArrayList<String>();
		HashMap<Integer,Integer> positionList = new HashMap<Integer,Integer>();
		*/

		tweetList = new long[tweetCount];
		//ArrayList<Long> tweetList = new ArrayList<Long>();
		
		File dir = new File(myDirectoryPath);
		File[] directoryListing = dir.listFiles();
		Date date1 = new Date();
		int locationIndex = 0;
		int dateIndex = 0;
		
		if(directoryListing != null)
		{
			int lineCount = 0;
			//build hashmap for location and date
			for (File child : directoryListing) 
			{
				Scanner scan = new Scanner(child);
				String part1,content;
				String rank;
				scan.useDelimiter("[\t\n]");
				while(scan.hasNext())
				{
					lineCount ++;
					part1 = scan.next();
					content = scan.next();
					rank = scan.next();
					int rankValue = Integer.valueOf(rank);
					String date = part1.substring(0, 10).replaceAll("-", "");
					int dateValue = Integer.valueOf(date);
					if(!dateMap.containsKey(dateValue))
					{
						dateMap.put(dateValue, dateIndex);
						dateIndex ++;
					}
					String location = part1.substring(11);
					if(!locationMap.containsKey(location))
					{
						locationMap.put(location, locationIndex);
						locationIndex ++;
					}
				}
				scan.close();
			}
			int dateSize = dateMap.size();
			int locationSize = locationMap.size();
			
			valuePos = new int[dateSize][locationSize];
			valueList = new String[lineCount];
			valuePosList = new int[lineCount];
			
			int fileIndex = 0;
			int lineIndex = 0;
			int tweetIndex = 0;
			
			long tempTweet = 0;
			for (File child : directoryListing) 
			{
				fileIndex ++;
				System.out.println("Processing " + fileIndex);
				Scanner scan = new Scanner(child);
				String part1,content;
				String rank;
				scan.useDelimiter("[\t\n]");
				while(scan.hasNext())
				{
					part1 = scan.next();
					content = scan.next();
					rank = scan.next();
					int rankValue = Integer.valueOf(rank);
					if(rankValue == 1)
					{
						String date = part1.substring(0, 10).replaceAll("-", "");
						int dateValue = dateMap.get(Integer.valueOf(date));								
					
						String location = part1.substring(11);
						int locationValue = locationMap.get(location);
						valuePos[dateValue][locationValue] = lineIndex;
						
					}				
					// split value
					int index1 = content.indexOf(':');
					String tempValue = content.substring(0, index1);
					valueList[lineIndex] = tempValue;
					valuePosList[lineIndex] = tweetIndex;
					// split tweet
					int preIndex = index1 + 1;
					for(int i = index1 + 1;i < content.length();i ++)
					{
						if(content.charAt(i) == ',')
						{
							tempTweet = Long.valueOf(content.substring(preIndex, i));
							//tweetList.add(tempTweet);
							tweetList[tweetIndex] = tempTweet;
							preIndex = i + 1;
							tweetIndex ++;
						}
						if(i == content.length() - 1)
						{

							tempTweet = Long.valueOf(content.substring(preIndex, i + 1));
							//tweetList.add(tempTweet);
							tweetList[tweetIndex] = tempTweet;
							tweetIndex ++;
						}
					}
					lineIndex ++;
				}
				scan.close();
			}
			System.out.println("tweet count" + tweetIndex);
		}
		else
		{
			valuePos = new int[1][1];
			valueList = new String[1];
			valuePosList = new int[1];
		}
		Date date2 = new Date();
		System.out.println("Time elapsed:" +(date2.getTime() - date1.getTime()) / 1000.0);
		//System.out.println("Content size:" + contentList.size());
		System.out.println("Build success");
		memory = runtime.totalMemory() - runtime.freeMemory();
		System.out.println("Used memory is megabytes: " + memory / MEGABYTE);
	}
	public String getAnswer(String date,String location,int m,int n)
	{
		System.out.println(date);
		System.out.println(location);
		System.out.println(m);
		System.out.println(n);
		String result = "";
		int dateValue = dateMap.get(Integer.valueOf(date));									
		int locationValue = locationMap.get(location);
		
		int valueIndex = valuePos[dateValue][locationValue];
		int startIndex = valueIndex + m -1;
		int endIndex = valueIndex + n -1;
		for(int i = startIndex;i <= endIndex ;i ++)
		{
			result += valueList[i] + ":";
			int tweetStartIndex = valuePosList[i];
			int tweetEndIndex = -1;
			if(i == valuePosList.length - 1)
			{
				tweetEndIndex = tweetList.length - 1;
			}
			else
			{
				tweetEndIndex =  valuePosList[i + 1] -1;
			}
			
			for(int j = tweetStartIndex; j <= tweetEndIndex ;j ++)
			{
				if(j == tweetEndIndex)
					result += String.valueOf(tweetList[j])+ "\n";
				else
					result += String.valueOf(tweetList[j]) + ",";
			}
		}
		System.out.println(result);
		return result;
	}

}

