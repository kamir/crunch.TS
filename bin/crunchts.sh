#!/bin/sh

export P1=-explode
export P2=/TSBASE/EXP1
export P3=Components_DAX2_Close__2003_2004_2005_2006_2007_2008_2009_2010_2011_2012_2013_2014.tsb.vec.seq
export P4=pairs.avro

echo "CMD   : $P1"
echo "Input : $P2/$P3"
echo "Output: $P4"

hadoop jar target/SparkNetworkCreator-0.1.0-SNAPSHOT-job $1 $2/$3 $4 
