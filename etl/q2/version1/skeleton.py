add jar s3://some/bucket/package_name.jar;


CREATE TEMPORARY FUNCTION fucntion_name as 'package_name.ClassName';

create database testdb;
use testdb;
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
location 's3://input/data' ;

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

insert overwrite table target_table_name
select 
field1, fucntion_name(field_name) as field2
from source_table_name;
