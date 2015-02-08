#!/usr/bin/python
# -*- coding: UTF-8 -*-
import sys
import json

for line in sys.stdin:
	newline = unicode(line, 'utf-8')
	try:
		rawdata = json.loads(newline)
	except ValueError:
		continue
	tweet_id = 0
	user_id = 0
	try:
		tweet_id = rawdata['id']
	except ValueError:
		continue
	try:
		user_id = rawdata['user']['id']
	except ValueError:
		continue
	#retweeted_status = None
	orig_user_id = -1
	if 'retweeted_status' in rawdata:
		try:
			orig_user_id = rawdata['retweeted_status']['user']['id']
		except ValueError:
			orig_user_id = -1
	if orig_user_id != -1:
	#print '%s\t%s\t%s' %(user_id.encode('utf-8'), tweet_id.encode('utf-8'), orig_user_id.encode('utf-8'))
		print '%d\t%d\t%d' %(orig_user_id, tweet_id, user_id)	
	print '%d\t%d\t%d' %(user_id, tweet_id, -1)