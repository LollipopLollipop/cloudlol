#!/bin/bash
#install Sqoop 
cd /home/hadoop 
hadoop fs -copyToLocal s3://cloudlol15619/sqoop-1.4.5.bin__hadoop-2.0.4-alpha.tar.gz sqoop-1.4.5.bin__hadoop-2.0.4-alpha.tar.gz
tar -xzf sqoop-1.4.5.bin__hadoop-2.0.4-alpha.tar.gz
hadoop fs -copyToLocal s3://cloudlol15619/mysql-connector-java-5.1.33.tar.gz mysql-connector-java-5.1.33.tar.gz
tar -xzf mysql-connector-java-5.1.33.tar.gz
cp mysql-connector-java-5.1.33/mysql-connector-java-5.1.33-bin.jar sqoop-1.4.5.bin__hadoop-2.0.4-alpha/lib/