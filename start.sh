#!/bin/bash

# Start ZooKeeper in the background
/usr/share/zookeeper/bin/zkServer.sh start

# Wait 3 seconds for ZooKeeper to initialize
sleep 3

# Run the Java application
exec java -jar /app/dbq/coordinator/target/coordinator-0.1.0-SNAPSHOT.jar
