#!/usr/bin/python
import sys
import os
#indir = '/Users/JodiezZ/Desktop/15619/Projects/15619Project/phase3/q5/mapperout/'
indir = '/Users/JodiezZ/Desktop/15619/Projects/15619Project/phase3/q5/workingpool/'
photo_count_dict= dict()
#retweet_id_dict = dict()
#retweet_user_dict = dict()
for root, dirs, files in os.walk(indir):
	for f in files:
		if f[:4] != 'part':
			continue
		inFile = open(f,'r')
		for newline in inFile:
			try:
				user_id = newline.split('\t')[0].strip()
				tweet_id = int(newline.split('\t')[1].strip())
				photo_count = int(newline.split('\t')[2].strip())
			except IndexError:
				continue
			except ValueError:
				continue
			if user_id not in photo_count_dict:
				photo_count_dict[user_id] = dict()
			photo_count_dict[user_id][tweet_id] = photo_count

for usr in photo_count_dict:
	count = 0
	for tid in photo_count_dict[usr]:
		count = count + photo_count_dict[usr][tid]
	print '%s\t%d' %(usr, count)