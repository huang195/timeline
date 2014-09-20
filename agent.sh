#!/bin/bash

export namespace="cloud3.0"
export origin="tlagent"

# Redirect stdout to ./tmp to keep script cleaner
export f="./tmp.1"
exec > $f

# Collecing all files on the file system and put them into json format
# /root /sys /proc directories are skipped
# file name, last modified time, and file size are printed for each file
echo "{"
echo "	\"time\": " "\"`date`\","
echo "	\"files\": ["
find / -mount \( -path /root -o -path /sys -o -path /proc \) -prune -o -printf "\t\t{\"name\": \"%p\", \"lmt\": \"%c\", \"size\": \"%k\"},\n"
echo "	]"
echo "}"

# Remove the last comma in the file to make it json-compatible
sed -i ':a;N;$!ba;s/},\n\t\]/}\n\t\]/g' $f

curl -X POST --data-binary @$f http://localhost:10252/put/${namespace}/${origin} --header "Content-Type:application/json"
