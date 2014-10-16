#!/usr/local/bin/python

####################################################################################
#
# Finds noise from file system that should ignored when changed
#
# Saves state in TL_DATA_DIR/<namespace>/<origin>/.nm so it can pick up where it 
# left off the last time it ran
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

# These factors below are configurable to determine what is noise, and they are all 
# in units of DEFAULT_FREQ

# COLLECTION_TOO_FAR_FACTOR determines when two consecutive collection points are too
# away to be used to determine noise (e.g., when the collection agent malfunctions).
# Bigger values might find more noises
COLLECTION_TOO_FAR_FACTOR = 3

# FILE_NO_LONGER_CHANGING_FACTOR determines when we should stop tracking a file after 
# it was last modified. This is to improve memory usage of the noisemachine. Bigger value
# will use more memory but maybe more accurate
FILE_NO_LONGER_CHANGING_FACTOR = 5

# CHANGE_FREQUENCY_FUZZY_FACTOR allows files to be modified at a slightly lower frequency
# than what is specified by DEFAULT_FREQ. Bigger values find more noises
CHANGE_FREQUENCY_FUZZY_FACTOR = 1.5

# INCUBATION_FACTOR specifies how long we need to wait till we are certain a file is noisy.
# Smaller values find more noises
INCUBATION_FACTOR = 10

####################################################################################
#
# Functions
#
####################################################################################

def findNoise(namespace, source, output, freq):

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
	#      'firstmodifiedtime_dt' : '2014-09-24T10:43:00Z',  <- when file was first registered
	#      'lastmodifiedtime_dt' : '2014-09-24T10:43:00Z',  <- when file was last modified
	#      'collection_dt' : '2014-09-24T10:43:00Z',  <- when the last collection happened
	#      'intervals' : [15, 20, 20], <- modified intervals
	#     }
	#   ]
	#

	try:
		f = open(baseDirName + '/.nm', 'r')
		dataframe = json.loads(f.read())
	except:
		dataframe = { 'index': 1, 'docs': 0, 'files': []}
	finally:
		f.close()

	for i in range(dataframe['index'], index):
		try:
			f = open(baseDirName + '/' + str(i) + '.json', 'r')
			count = 0
			j = []
			for line in f:
				# read only even lines
				count += 1
				if count % 2 == 1:
					continue
				j.append(json.loads(line))

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
						if sec > FILE_NO_LONGER_CHANGING_FACTOR * freq:
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
				if sec <= COLLECTION_TOO_FAR_FACTOR * freq:
					time1 = datetime.strptime(file['lastmodifiedtime_dt'], '%Y-%m-%dT%H:%M:%SZ')
					time2 = datetime.strptime(d['lastmodifiedtime_dt'],'%Y-%m-%dT%H:%M:%SZ')
					delta = time2 - time1 
					sec = delta.total_seconds()
					file['intervals'].append(sec)

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

	f = None
	if output != None:
		f = open(output, "w")
	else:
		f = sys.stdout

	f.write('[\n')
	for file in dataframe['files']:

		# skip if there's not enough info on the file
		if len(file['intervals']) == 0:
			continue

		# skip if file change interval is too long
		exceptions = 0
		for i in file['intervals']:
			if i > CHANGE_FREQUENCY_FUZZY_FACTOR * freq:
				exceptions += 1
		if exceptions > 0:
			continue

		# skip if data has not been collected long enough
		time1 = datetime.strptime(file['firstmodifiedtime_dt'], '%Y-%m-%dT%H:%M:%SZ')
		time2 = datetime.strptime(file['lastmodifiedtime_dt'],'%Y-%m-%dT%H:%M:%SZ')
		delta = time2 - time1 
		sec = delta.total_seconds()
		if sec < INCUBATION_FACTOR * freq:
			continue

		f.write('\t{{\"name_s\": \"{0}\"}},\n'.format(file['name_s']))
	f.write(']\n')
	f.close()

	f = open(baseDirName + '/.nm', 'w')
	json.dump(dataframe, f, indent=2)
	f.close()

	#print json.dumps(dataframe,indent=2)

def parseArgs(argv):
	
	namespace = ''
	source = ''
	freq = DEFAULT_FREQ
	output = None

	try:
		opts, args = getopt.getopt(argv,'n:s:o:f:',['namespace=','source=', 'output=', 'freq='])
	except getopt.GetoptError:
		print 'noisemachine.py -n <namespace> -s <source> [-o <output> -f <freq>]'
		sys.exit(2)

	for opt, arg in opts:
		if opt in ("-n", "--namespace"):
			namespace = arg
		elif opt in ("-s", "--source"):
			source = arg
		elif opt in ("-o", "--output"):
			output = arg
		elif opt in ("-f", "--freq"):
			freq = int(arg)

	if namespace != '' and source != '':
		return (namespace, source, output, freq)
	else:
		print 'noisemachine.py -n <namespace> -s <source> [-o <output> -f <freq>]'
		sys.exit(2)

if __name__ == '__main__':

	namespace, source, output, freq = parseArgs(sys.argv[1:])
	print 'Starting Noise Machine process({0}) for namespace({1}) and source({2})'.format(os.getpid(),namespace,source)
	print 'Log output will be stored in {0}'.format(NM_LOG)
	logging.basicConfig(filename=NM_LOG, filemode='w', format='%(asctime)s %(levelname)s : %(message)s', level=logging.DEBUG)
	logger = logging.getLogger(__name__)
	logger.info('Started Noise Machine process({0})'.format(os.getpid()))
	logger.info('Log output will be stored in {0}'.format(NM_LOG))
	findNoise(namespace, source, output, freq)
