add jar s3://elasticmapreduce/samples/hive-ads/libs/jsonserde.jar;
add jar s3://cloudlol15619/lib/sentiscore.jar;
add jar s3://cloudlol15619/lib/censortext.jar;
add jar s3://cloudlol15619/lib/converttime.jar;


CREATE TEMPORARY FUNCTION censorText as 'textcensoring.TextCensoring';
CREATE TEMPORARY FUNCTION calcSentiment as 'sentimentscoring.SentimentScoring';
CREATE TEMPORARY FUNCTION convertTime as 'converttime.ConverTimeFormat';

create database octsdb;
use octsdb;
create external table if not exists tweetdata (
	tweet_id bigint, 
	tweet_text string, 
	created_at string, 
	user_id bigint
)
row format serde 'com.amazon.elasticmapreduce.JsonSerde'
with serdeproperties ( 
	'paths'='id,text,created_at,user.id'
)
location 's3://15619f14twittertest2/600GB' ;

create table tweetinfo (
	tweet_id bigint,
	created_at string,
	user_id bigint,
	censored_text string,
	sentiment_score bigint
)
row format delimited fields terminated by '\001'
lines terminated by '\n'
stored as textfile ;

insert overwrite table tweetinfo 
select 
tweet_id, convertTime(created_at) as created_at, user_id, censorText(regexp_replace(tweet_text, '\\n|\\r', '\002')) as censored_text, calcSentiment(tweet_text) as sentiment_score
from tweetdata;
