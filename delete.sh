#!/bin/sh

# Example: ./delete.es.sh cloud3.0

if [ -z $1 ]
then
	echo "usage: ./delete.es.sh <index name>"
	exit 1
fi

curl -X DELETE http://localhost:9200/$1

