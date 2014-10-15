#!/bin/sh

if [ -z $1 ]
then
	echo "usage: ./delete.solr.sh <index name>"
	exit 1
fi

curl -X POST --data-binary "<delete><query>*:*</query></delete>" http://localhost:8983/solr/update?commit=true -H "Content-Type: text/xml"

