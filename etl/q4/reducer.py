#!/usr/bin/python
# -*- coding: UTF-8 -*-
import sys
import re
import codecs
import urllib2
import operator

prev_key = ""
prev_hashtag_content = ""
prev_tweet_id = 0
tweets_count = 0
tweets_collection = list()
hashtag_collection = list()
for line in sys.stdin:
	newline = unicode(line, 'utf-8')
	try:
		compound_key = newline.split('\t')[0].strip()
		compound_value = newline.split('\t')[1].strip()
		hashtag_content = compound_value.split(';')[0].strip()
		tweet_id = int(compound_value.split(';')[1].strip())
		hashtag_index = compound_value.split(';')[2].strip()
	except IndexError:
		continue
	except ValueError:
		continue
	if compound_key != prev_key or hashtag_content != prev_hashtag_content:
		if prev_hashtag_content != "":
			#sorted_tweets_collection = sorted(tweets_collection.items())
G		hashtag_collection.append([-tweets_count, tweets_collection, prev_hashtag_content])
			tweets_count = 0
		#update hashtag and tweet list 
		prev_hashtag_content = hashtag_content
		tweets_collection = list()
		tweets_count = tweets_count + 1
		tweets_collection.append([tweet_id,int(re.split(r'\W+',hashtag_index)[1])])
		if compound_key != prev_key:
			if prev_key != "":
				hashtag_collection.sort()
				#print len(sorted_hashtag_collection)
				compound_value = ""
				for i in range(0,len(hashtag_collection)):
					#print "to print"
					compound_value = hashtag_collection[i][2]+':<'
					for j in range(0,len(hashtag_collection[i][1])):
						compound_value = compound_value+str(hashtag_collection[i][1][j][0])+','
					compound_value = compound_value[:-1]
					compound_value = compound_value + '>'
					output_line = prev_key+'\t'+str(i+1)+'\t'+compound_value
					print output_line.encode('utf-8')
			prev_key = compound_key
			hashtag_collection = list()
	else:	
		#ignore duplicate hashtag in one tweet 
		if tweet_id == prev_tweet_id:
			continue
		#group tweets for the same hashtag together, keep index for further filtering
		tweets_count = tweets_count + 1
		tweets_collection.append([tweet_id,int(re.split(r'\W+',hashtag_index)[1])])
	prev_tweet_id = tweet_id

hashtag_collection.append([-tweets_count, tweets_collection, prev_hashtag_content])
tweets_count = 0
hashtag_collection.sort()
compound_value = ""
for i in range(0,len(hashtag_collection)):
	compound_value = hashtag_collection[i][2]+':<'
	for j in range(0,len(hashtag_collection[i][1])):
		compound_value = compound_value+str(hashtag_collection[i][1][j][0])+','
	compound_value = compound_value[:-1]
	compound_value = compound_value + '>'
	output_line = prev_key+'\t'+str(i+1)+'\t'+compound_value
	print output_line.encode('utf-8')
