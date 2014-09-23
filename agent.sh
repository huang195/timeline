#!/bin/bash

#################################################
#
# usage: ./agent.sh [directory=`pwd`]
#
#################################################

#################################################
#	Configurable variables
#################################################

namespace="cloud3.0"
origin="tlagent"
compressed=1

dir=`pwd`
if [ -n "$1" ]
then
	dir=$1
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
find $dir -mount \( -path /root -o -path /sys -o -path /proc \) -prune -o -printf "\t\t{\"name_s\": \"%p\", \"lastmodifiedtime_dt\": \"%TY-%Tm-%TdT%TH:%TM:00Z\", \"size_i\": \"%k\", \"permission_s\": \"%M\", \"type_s\": \"%Y\"},\n"
echo "	]"
echo "}"

# Remove the last comma in the file to make it json-compatible
sed -i ':a;N;$!ba;s/},\n\t\]/}\n\t\]/g' $f

if [ $compressed == 1 ]
then
	rm -f $f.gz
	gzip $f
	curl -X POST --data-binary @$f.gz http://localhost:10252/put/${namespace}/${origin}?compressed=true --header "Content-Type:application/json" > agent.log
else
	curl -X POST --data-binary @$f http://localhost:10252/put/${namespace}/${origin} --header "Content-Type:application/json" > agent.log
fi

