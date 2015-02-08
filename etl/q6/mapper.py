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
	user_id = ""
	photo_count = 0
	tweet_id = ""
	try:
		user_id = rawdata['user']['id_str'].strip()
	except ValueError:
		continue
	try:
		tweet_id = rawdata['id_str'].strip()
	except ValueError:
		continue
	user_id = user_id.zfill(19)
	entities = None
	try:
		entities = rawdata['entities']
	except ValueError:
		continue
	if 'media' not in entities:
		continue
	media_array = None
	try:
		media_array = entities['media']
	except ValueError:
		continue
#	if len(media_array) > 1:
#		print user_id
	for media_object in media_array:
		if media_object['type'] == 'photo':
			photo_count = photo_count + 1
	if photo_count > 0:
		print '%s\t%s\t%d' %(user_id, tweet_id, photo_count)