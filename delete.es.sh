#!/bin/sh

# Example: ./delete.es.sh cloud3.0

curl -X DELETE http://localhost:9200/$1

