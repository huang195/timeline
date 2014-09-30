#!/bin/sh

curl -X POST --data-binary "<delete><query>*:*</query></delete>" http://localhost:8983/solr/update?commit=true -H "Content-Type: text/xml"

