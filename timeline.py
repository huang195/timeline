#!/usr/local/bin/python

####################################################################################
#
# Provides Time Line (TL) REST services
#
####################################################################################

import os
import sys
import traceback
import logging
import multiprocessing
import bottle
import re
import subprocess
import gzip
import StringIO
import TLData
import TLDiff

try: import simplejson as json
except ImportError: import json

####################################################################################
# 
# Globals
#
####################################################################################

app = bottle.Bottle()
logger = None

apihelp = '''
TL REST calls
-----------

1. /put

   Put data into time line 

2. /get[?time=time]

   Get data from time line specified by time parameter. If no time parameter
   is given, the data from the latest time is returned  

3. /check

   Hash of raw data is sent via payload, and for each, we return whether or not
   hash values match those of the latest data. This avoids having to send data
   that has not changed since the last time point.

4. /diff[?from=time1][&to=time2]

   Return the differences between time1 and time2

5. /help
   
   Show this API help
'''

####################################################################################
#
# Configurations
#
####################################################################################

TL_HOST		=	'0.0.0.0'
TL_PORT		=	10252
TL_LOG		=	'logs/tl.log'
INDEXER		=	'ElasticSearch'	#'Solr'

####################################################################################
#
# Functions
#
####################################################################################

# return API help
@app.route("/")
@app.route("/help")
def help():
	''' print help page '''
	bottle.response.content_type = 'text/plain'
	return apihelp

@app.route("/put/<namespace>/<source>", method="POST")
def put(namespace, source):
	''' save data '''

	bottle.response.content_type = 'text/json'

	namespaceInvalid = re.match('^[\w\-\.]+$', namespace) is None
	if namespaceInvalid:
		logger.error("Unexpected namespace: {0}".format(namespace))
		return json.dumps({'success': False, 'stacktrace': traceback.format_exc().split('\n')}, indent=2)

	sourceInvalid = re.match('^[\w-]+$', source) is None
	if sourceInvalid:
		logger.error("Unexpected source: {0}".format(source))
		return json.dumps({'success': False, 'stacktrace': traceback.format_exc().split('\n')}, indent=2)

	# Check if data is compressed
	compressed = False
	if bottle.request.query:
		value = bottle.request.query.get('compressed', None)
		if value:
			if value.lower() == 'true':
				compressed = True
			else:
				compressed = False

	data = ''
	dataSize = bottle.request.content_length
	if (dataSize < bottle.request.MEMFILE_MAX):
		# req.body in a string if its size is smaller than MEMFILE_MAX
		data = bottle.request.body.getvalue()
	else:
		# req.body in a file if its size is greater than MEMFLE_MAX
		data = bottle.request.body.read()

	# Find current index for (namespace,origin)
	tli = TLData.TLIndex(namespace, source)
	baseDatafileName = tli.getBaseDatafileName()
	tli.incrementIndex()

	# Write out raw data collected by the agent
	tlrd = TLData.TLRawData()
	p = multiprocessing.Process(target=tlrd.write, args=(baseDatafileName, data, compressed))
	p.start()
	p.join()

	# Flatten out raw data and write it out, which can be used as input to Solr
	#tlrdi = TLData.TLRawDataIndex()
	#p = multiprocessing.Process(target=tlrdi.write, args=(baseDatafileName, data, compressed))
	#p.start()
	#p.join()

	# Write out diff of raw data
	tld = TLDiff.TLDiffData()
	p = multiprocessing.Process(target=tld.write, args=(baseDatafileName,))
	p.start()
	p.join()

	# Flatten out raw diff data and write it out, which can be used as input to Solr
	if INDEXER == 'Solr':
		tldi = TLDiff.TLDiffDataIndexSolr()
		p = multiprocessing.Process(target=tldi.write, args=(baseDatafileName,))
		p.daemon = True
		p.start()
	elif INDEXER == 'ElasticSearch':
		tldi = TLDiff.TLDiffDataIndexES()
		p = multiprocessing.Process(target=tldi.write, args=(baseDatafileName,))
		p.daemon = True
		p.start()
	else:
		logger.error('Indexer {0} not supported'.format(INDEXER))

	return json.dumps({'success': True})
	

# Main listen/exec loop

if __name__ == '__main__':
	if not os.path.exists('logs'):
		os.makedirs('logs')

	print 'Starting Time Line process({0})'.format(os.getpid())
	print 'Log output will be stored in {0}'.format(TL_LOG)
	logging.basicConfig(filename=TL_LOG, filemode='w', format='%(asctime)s %(levelname)s : %(message)s', level=logging.DEBUG)
	logger = logging.getLogger(__name__)
	logger.info('Started Time Line process({0})'.format(os.getpid()))
	logger.info('Log output will be stored in {0}'.format(TL_LOG))
	app.run(host=TL_HOST, port=TL_PORT, quiet=True)
