#!/bin/sh

if [ -z $1 ]
then
	echo "usage: ./index.solr.sh <filename>"
	exit 1
fi

curl -X POST --data-binary @$1 http://localhost:8983/solr/update?json.command=false\&commit=true -H "Content-Type: text/json"

