#!/usr/bin/python
# -*- coding: UTF-8 -*-
import sys
import json
import re

for line in sys.stdin:
	newline = unicode(line, 'utf-8')
	try:
		rawdata = json.loads(newline)
	except ValueError:
		continue
	user_id = rawdata['user']['id_str'].strip()
	if user_id == "":
		continue
	created_at = rawdata['created_at'].strip()
	if created_at == "":
		continue
	tweet_content = rawdata['text']
	try:
		tweet_id = rawdata['id_str'].strip()
	except ValueError:
		continue
	#achieve numerical sorting based on lexicographical sorting
	tweet_id = tweet_id.zfill(19)
	weekday, month, date, time, other, year = created_at.split(' ')
	month = month.strip()
	date = date.strip()
	time = time.strip()
	year = year.strip()
	if month == 'Jan':
		monthDigit = '01'
	elif month == 'Feb':
		monthDigit = '02'
	elif month == 'Mar':
		monthDigit = '03'
	elif month == 'Apr':
		monthDigit = '04'
	elif month == 'May':
		monthDigit = '05'
	elif month == 'Jun':
		monthDigit = '06'
	elif month == 'Jul':
		monthDigit = '07'
	elif month == 'Aug':
		monthDigit = '08'
	elif month == 'Sep':
		monthDigit = '09'
	elif month == 'Oct':
		monthDigit = '10'
	elif month == 'Nov':
		monthDigit = '11'
	else:
		monthDigit = '12'
	tweet_time = year+'-'+monthDigit+'-'+date+'+'+time
	compound_key = user_id+";"+tweet_time+'/'+tweet_id
	tweet_content = re.sub(r"\n\r|\r\n",'\n',tweet_content)
	tweet_content = re.sub(r"\n|\r",'\002',tweet_content)
	print '%s\t%s' %(compound_key.encode('utf-8'), tweet_content.encode('utf-8'))
