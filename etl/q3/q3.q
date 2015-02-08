add jar s3://elasticmapreduce/samples/hive-ads/libs/jsonserde.jar;

create database novadb;
use novadb;
create external table if not exists retweetdata (
	user_id bigint,
	orig_user_id bigint
)
row format serde 'com.amazon.elasticmapreduce.JsonSerde'
with serdeproperties ( 
	'paths'='user.id,retweeted_status.user.id'
)
location 's3://cloudlol15619/tables/';

create table retweetfrom (
	user_id bigint,
	retweet_from bigint
);


insert overwrite table retweetfrom 
select
*
from retweetdata
where user_id is not null;

create table retweetby (
	user_id bigint,
	retweet_by bigint
);

insert overwrite table retweetby
select 
orig_user_id as user_id, 
user_id as retweet_by
from retweetdata
where orig_user_id is not null;

create table retweetunion (
	user_id bigint,
	retweet_from bigint,
	retweet_by bigint
)
row format delimited fields terminated by '\001'
lines terminated by '\n'
stored as textfile
location 's3://cloudlol15619/phase2q3output/';

insert overwrite table retweetunion
select retweetby.user_id, retweetby.retweet_by, retweetfrom.retweet_from
from retweetby left join retweetfrom on retweetby.user_id = retweetfrom.user_id;

