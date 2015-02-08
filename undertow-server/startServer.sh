mvn assembly:assembly -DdescriptorId=jar-with-dependencies
sudo java -Xms7300M -Xmx7300M -jar target/undertow-server-1.0-SNAPSHOT-jar-with-dependencies.jar
