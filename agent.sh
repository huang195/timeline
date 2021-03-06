#!/bin/bash

#################################################
#
# usage: ./agent.sh <namespace> <source> [directory=`pwd`]
#
#################################################

#################################################
#	Configurable variables
#################################################

compressed=1
timelineserver="localhost:10252"

if [ -z "$1" ]
then
	echo "usage: agent.sh namespace source [rootdir]"
	exit 1
else
	namespace=$1
fi

if [ -z "$2" ]
then
	echo "usage: agent.sh namespace source [rootdir]"
	exit 1
else
	origin=$2
fi

dir=`pwd`
if [ -n "$3" ]
then
	dir=$3
fi

# Redirect stdout to ./tmp to keep script cleaner
export f="./tmp.1"
exec >$f

# Collecing all files on the file system and put them into json format
# /root /sys /proc directories are skipped
# file name, last modified time, and file size are printed for each file
echo "{"
echo "	\"hostname_s\": " "\"`hostname`\","
echo "	\"collection_dt\": " "\"`date +%Y-%m-%dT%H:%M:%SZ`\","
echo "	\"files\": ["
find $dir -mount \( -path /root -o -path /sys -o -path /proc \) -prune -o -printf "\t\t{\"name_s\": \"%p\", \"lastmodifiedtime_dt\": \"%CY-%Cm-%CdT%CH:%CM:00Z\", \"size_i\": \"%s\", \"permission_s\": \"%M\", \"type_s\": \"%Y\"},\n"
echo "	]"
echo "}"

# Remove the last comma in the file to make it json-compatible
sed -i ':a;N;$!ba;s/},\n\t\]/}\n\t\]/g' $f

if [ $compressed == 1 ]
then
	gzip $f
	curl -X POST --data-binary @$f.gz http://${timelineserver}/put/${namespace}/${origin}?compressed=true --header "Content-Type:application/json" > agent.log
	rm -f $f.gz
else
	curl -X POST --data-binary @$f http://${timelineserver}/put/${namespace}/${origin} --header "Content-Type:application/json" > agent.log
	rm -f $f
fi

