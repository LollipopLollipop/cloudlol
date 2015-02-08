#!/usr/bin/python
import sys

tweet_id_set = set()
retweet_id_set = set()
retweet_user_set = set()
prev_user_id_1 = -1
user_id_1 = -1
#count = 0
for newline in sys.stdin:
	#newline = unicode(line, 'utf-8')
	try:
		user_id_1 = int(newline.split('\t')[0].strip())
		tweet_id = int(newline.split('\t')[1].strip())
		user_id_2 = int(newline.split('\t')[2].strip())
	except IndexError:
		continue
	except ValueError:
		continue
	if user_id_1 != prev_user_id_1:
		if prev_user_id_1 != -1:
			s1 = len(tweet_id_set)
			s2 = 3*len(retweet_id_set)
			s3 = 10*len(retweet_user_set)
			total = s1+s2+s3
#			if s1 == 0:
#				count = count + 1
			print '%d\t%d\t%d\t%d\t%d' %(prev_user_id_1, s1, s2, s3, total)
		prev_user_id_1 = user_id_1
		tweet_id_set = set()
		retweet_id_set = set()
		retweet_user_set = set()
	if user_id_2 == -1:
		tweet_id_set.add(tweet_id)
	else:
		retweet_id_set.add(tweet_id)
		retweet_user_set.add(user_id_2)
	
s1 = len(tweet_id_set)
s2 = 3*len(retweet_id_set)
s3 = 10*len(retweet_user_set)
total = s1+s2+s3
#if s1 == 0:
#	count = count + 1
print '%d\t%d\t%d\t%d\t%d' %(user_id_1, s1, s2, s3, total)
#print count
