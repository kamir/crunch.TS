#!/bin/sh

echo "CMD   : $1"
echo "Input : $2/$3"
echo "Output: $4"

hadoop jar target/crunchTS-0.0.1-SNAPSHOT-job.jar $1 $2/$3 $4 
