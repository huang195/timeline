#!/bin/sh

curl -X POST --data-binary @$1 http://localhost:9200/_bulk

