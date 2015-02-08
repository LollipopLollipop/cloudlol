#!/usr/bin/python
# -*- coding: UTF-8 -*-
import sys
import re
import codecs
import urllib2

sentimentFile = urllib2.urlopen("https://s3.amazonaws.com/F14CloudTwitterData/AFINN.txt");
sentimentScore = dict()
for line in sentimentFile:
  line = line.strip()
  parts = [p.strip() for p in line.split("\t")]
  sentimentScore[parts[0]] = parts[1]

bannedWordsFile = urllib2.urlopen("https://s3.amazonaws.com/F14CloudTwitterData/banned.txt")
bannedWords = set()
for line in bannedWordsFile:
	enc = codecs.getencoder("rot-13")
	target = str(enc(line.strip())[0])
	bannedWords.add(target)

def asterisk(m):
	sub_len = len(m.group(2))
	return m.group(1) + "".join('*' for x in range(0,sub_len))+m.group(3)

prev_user_time= ""
#prev_tweet_time = ""
#prev_user_id = ""
prev_tweet_id = 0
aggr_value = ""
for line in sys.stdin:
	newline = unicode(line, 'utf-8')
	compound_key = newline.split('\t')[0]
	try:
		user_time= compound_key.split('/')[0].strip()
		#user_id = user_time.split(';')[0].strip()
		#tweet_time = user_time.split(';')[1].strip()
		tweet_id = int(compound_key.split('/')[1].strip())
	except IndexError:
		continue
	except ValueError:
		continue
	#no action for duplicate tweets
	if user_time == prev_user_time and tweet_id == prev_tweet_id:
		continue
	key_length = len(compound_key)
	whole_length = len(newline)
	tweet_content = newline[(key_length-whole_length+1):][:-1]
	censored_text = tweet_content
	tweet_word_count = len(re.split(r"\W+|_|\b",tweet_content))
	final_score = 0
	for i in range(0,tweet_word_count):
		target = re.split(r"\W+|_|\b", tweet_content)[i]
		target_lower = target.lower()
		if target_lower in sentimentScore:
			final_score = final_score + int(sentimentScore[target_lower])
		if target_lower in bannedWords:
			target_repl = re.sub(r"(\w)(\w+)(\w)", asterisk, target)
			regex = re.compile(r"(\W|_|\b)(%s)(\W|_|\b)" % target)
			censored_text = regex.sub(r"\g<1>"+target_repl+"\g<3>", censored_text, 1)
	cur_value= str(tweet_id)+':'+str(final_score)+':'+censored_text+'\002'
	if user_time == prev_user_time:
		aggr_value = aggr_value + cur_value
	else:
		if prev_user_time != "":
			output_line = prev_user_time+'\001'+aggr_value
			print output_line.encode('utf-8')
		aggr_value = cur_value
		prev_user_time = user_time
		#prev_user_id = user_id
		#prev_tweet_time = tweet_time
	prev_tweet_id = tweet_id
	
	
output_line = user_time+'\001'+aggr_value
print output_line.encode('utf-8')