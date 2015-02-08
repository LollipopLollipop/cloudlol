#!/usr/bin/python
# -*- coding: UTF-8 -*-
import sys
import json
import re

for line in sys.stdin:
	#content = unicode(line.strip(codecs.BOM_UTF8), 'utf-8')
	newline = unicode(line, 'utf-8')
	try:
		rawdata = json.loads(newline)
	except ValueError:
		continue
	place_name = ""
	try:
		place = rawdata['place']
	except ValueError:
		place = None
	#print 'place:'+place
	if place != None:
		try: 
			place_name = place['name'].strip()
		except ValueError:
			place_name = ""
	#print 'place name:'+place_name
	if place_name == "":
		try:
			time_zone= rawdata['user']['time_zone']
		except ValueError:
			time_zone = None
	#print 'time zone:'+str(time_zone)
	if(place_name == "" and (re.search(r'\btime\b',str(time_zone),re.IGNORECASE|re.UNICODE) != None or time_zone == None)):
		continue
	elif (place_name == ""):
		location = str(time_zone)
	else:
		location = place_name

	try:
		hashtag = rawdata['entities']['hashtags']
		#hashtag_index = rawdata['entities']['hashtags']['indices']
	except ValueError:
		hashtag = None
	if hashtag == None:
		continue
	created_at = ""	
	try:
		created_at = rawdata['created_at'].strip()
	except ValueError:
		created_at = ""
	if created_at == "":
		continue
	try:
		tweet_id = rawdata['id_str'].strip()
	except ValueError:
		continue
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
	tweet_date = year+'-'+monthDigit+'-'+date
	compound_key = tweet_date+";"+location
	hashtag_size = len(hashtag)
	hashtag_text = ["" for x in range(hashtag_size)]
	hashtag_index = ["" for x in range(hashtag_size)]
	for i in range(0, hashtag_size):
		hashtag_text[i] = hashtag[i]['text']
		hashtag_index[i] = str(hashtag[i]['indices'])
		compound_value = hashtag_text[i]+';'+tweet_id+';'+hashtag_index[i]
		print '%s\t%s' %(compound_key.encode('utf-8'), compound_value.encode('utf-8'))
