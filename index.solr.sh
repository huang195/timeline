#!/bin/sh

curl -X POST --data-binary @$1 http://localhost:8983/solr/update?json.command=false\&commit=true -H "Content-Type: text/json"

