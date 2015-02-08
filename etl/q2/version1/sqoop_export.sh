#!/bin/bash

cd /home/hadoop/sqoop-1.4.5.bin__hadoop-2.0.4-alpha/bin

./sqoop export --connect jdbc:mysql://172.31.18.104:3306/phase1 --table tweetinfo --export-dir hdfs:///user/hive/warehouse/octfdb.db/tweetinfo --input-fields-terminated-by '\001' --input-lines-terminated-by '\n' --input-null-non-string '\\N' --username jodie --password cloudlol
