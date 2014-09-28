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
import re
import StringIO
import getopt

from datetime import datetime,timedelta

try: import simplejson as json
except ImportError: import json

####################################################################################
# 
# Globals
#
####################################################################################

logger = None
TL_DATA_DIR	=	'./data'
NM_LOG	=	'logs/nm.log'

####################################################################################
#
# Configurations
#
####################################################################################

# Files that are modified higher than this frequency will be labeled as noise (unit=sec)
DEFAULT_FREQ = 600

####################################################################################
#
# Functions
#
####################################################################################

def parseArgs(argv):
	
	namespace = ''
	source = ''
	freq = DEFAULT_FREQ

	try:
		opts, args = getopt.getopt(argv,"n:s:f:",["namespace=","source=", "freq="])
	except getopt.GetoptError:
		print 'noisemachine.py -n <namespace> -s <source> [-f <freq>]'
		sys.exit(2)

	for opt, arg in opts:
		if opt in ("-n", "--namespace"):
			namespace = arg
		elif opt in ("-s", "--source"):
			source = arg
		elif opt in ("-f", "--freq"):
			freq = int(arg)

	if namespace != '' and source != '':
		return (namespace, source, freq)
	else:
		print 'noisemachine.py -n <namespace> -s <source>'
		sys.exit(2)

def findNoise(namespace, source, freq):

	baseDirName = TL_DATA_DIR + '/' + namespace + '/' + source
	
	f = open(baseDirName + '/.metafile', 'r')
	index = int(f.read())
	f.close()

	if index <= 0:
		return

	# Example dataframe:
	#
	# {
	#   'index' : 20 <- index to next data file to ingest
	#   'docs'  : 15 <- number of data files ingested so far
	#   'files' : [
	#     {
	#      'name_s' : '/var/log/message',  <- name of the modified file
	#      'lastmodifiedtime_dt' : '2014-09-24T10:43:00Z',  <- last modified time
	#      'intervals' : [15, 20, 20], <- modified intervals
	#     }
	#   ]
	#
	# Logic:
	# 
	#  1. Use 'index' to find the next data file to ingest
	#  2. 'docs' is used to book-keep how many data files have been ingested so far

	dataframe = { 'index': 0, 'docs': 0, 'files': []}

	for i in range(1, index):
		try:
			f = open(baseDirName + '/' + str(i) + '.json', 'r')
			data = f.read()
			j = json.loads(data)

			if i == 1:
				# Just load the data into dataframe
				for m in j:
					# Skip non-modified files
					if m['change_s'].lower() != 'modified'.lower():
						continue

					file = {}
					file['name_s'] = m['name_s']
					file['lastmodifiedtime_dt'] = m['lastmodifiedtime_dt']
					file['firstmodifiedtime_dt'] = m['lastmodifiedtime_dt']
					file['collection_dt'] = m['collection_dt']
					file['intervals'] = []
					dataframe['files'].append(file)

				dataframe['index'] += 1
				dataframe['docs'] += 1
				continue

			# hash by filename
			j_h = {}
			for m in j:
				j_h[m['name_s']] = m

			# look through existing files
			collectionTime = None
			for file in dataframe['files']:
				if not file['name_s'] in j_h.keys():
					# get rid of files that are not noise
					if collectionTime != None:
						time1 = datetime.strptime(file['collection_dt'], '%Y-%m-%dT%H:%M:%SZ')
						time2 = datetime.strptime(collectionTime,'%Y-%m-%dT%H:%M:%SZ')
						delta = time2 - time1 
						sec = delta.total_seconds()
						if sec > 5 * DEFAULT_FREQ:
							#print 'deleting {0}'.format(file['name_s'])
							dataframe['files'].remove(file)
					continue

				d = j_h[file['name_s']]

				time1 = datetime.strptime(file['collection_dt'], '%Y-%m-%dT%H:%M:%SZ')
				time2 = datetime.strptime(d['collection_dt'],'%Y-%m-%dT%H:%M:%SZ')
				delta = time2 - time1 
				sec = delta.total_seconds()
				collectionTime = d['collection_dt']

				# if time between 2 collection points are too far, we dismiss it as it could be 
				# agent was intermittently not working
				if sec <= 3 * DEFAULT_FREQ:
					time1 = datetime.strptime(file['lastmodifiedtime_dt'], '%Y-%m-%dT%H:%M:%SZ')
					time2 = datetime.strptime(d['lastmodifiedtime_dt'],'%Y-%m-%dT%H:%M:%SZ')
					delta = time2 - time1 
					file['intervals'].append(delta.total_seconds())
				file['lastmodifiedtime_dt'] = d['lastmodifiedtime_dt']
				file['collection_dt'] = d['collection_dt']

				del j_h[file['name_s']]

			# add any newly identified files
			for d in j_h.values():
				# Skip non-modified files
				if d['change_s'].lower() != 'modified'.lower():
					continue

				file = {}
				file['name_s'] = d['name_s']
				file['lastmodifiedtime_dt'] = d['lastmodifiedtime_dt']
				file['firstmodifiedtime_dt'] = d['lastmodifiedtime_dt']
				file['collection_dt'] = d['collection_dt']
				file['intervals'] = []
				dataframe['files'].append(file)

			dataframe['index'] += 1
			dataframe['docs'] += 1

		except:
			# if file doesn't exist, we just keep going
			#print 'stacktrace: ', traceback.format_exc().split('\n')
			continue
		finally:
			f.close()

	for file in dataframe['files']:

		# skip if there's not enough info on the file
		if len(file['intervals']) == 0:
			continue

		# skip if file change interval is too long
		exceptions = 0
		for i in file['intervals']:
			if i > DEFAULT_FREQ:
				exceptions += 1

		if exceptions > 1:
			continue

		# skip if average file change interval is too long
		time1 = datetime.strptime(file['firstmodifiedtime_dt'], '%Y-%m-%dT%H:%M:%SZ')
		time2 = datetime.strptime(d['lastmodifiedtime_dt'],'%Y-%m-%dT%H:%M:%SZ')
		delta = time2 - time1 
		sec = delta.total_seconds()
		if sec / len(file['intervals']) > DEFAULT_FREQ:
			continue

		# skip if data has not been collected long enough
		if sec < DEFAULT_FREQ * 10:
			continue

		print file['name_s']

	#print json.dumps(dataframe,indent=2)

if __name__ == '__main__':

	namespace, source, freq = parseArgs(sys.argv[1:])

	print 'Starting Noise Machine process({0}) for namespace({1}) and source({2})'.format(os.getpid(),namespace,source)
	print 'Log output will be stored in {0}'.format(NM_LOG)
	logging.basicConfig(filename=NM_LOG, filemode='w', format='%(asctime)s %(levelname)s : %(message)s', level=logging.DEBUG)
	logger = logging.getLogger(__name__)
	logger.info('Started Noise Machine process({0})'.format(os.getpid()))
	logger.info('Log output will be stored in {0}'.format(NM_LOG))
	findNoise(namespace, source, freq)
