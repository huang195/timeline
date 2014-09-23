#!/usr/local/bin/python

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

try: import simplejson as json
except ImportError: import json

TL_DATA_DIR	=	'./data'

logger = logging.getLogger(__name__)

class TLIndex:
	'''
	'''

	def __init__(self, namespace, source):
		self.namespace = namespace
		self.source = source

		baseDirName = TL_DATA_DIR + '/' + self.namespace + '/' + self.source
		metafileName = baseDirName + '/' + '.metafile'

		if not os.path.isdir(baseDirName):
			try:
				logger.info("Creating directory {0}".format(baseDirName))
				os.makedirs(baseDirName)
			except:
				logger.error("Cannot create directory {0}".format(baseDirName))

		if not os.path.isfile(metafileName):
			try:
				logger.info("Creating metafile {0}".format(metafileName))
				metafile = open(metafileName, 'w')
				metafile.write('0')
				metafile.close
			except:
				logger.error("Cannot create metafile {0}".format(metafileName))

	def getBaseDatafileName(self):
		try:
			baseDirName = TL_DATA_DIR + '/' + self.namespace + '/' + self.source
			metafileName = baseDirName + '/' + '.metafile'
			metafile = open(metafileName, 'r')
			index = int(metafile.read())
			baseDatafileName = baseDirName + '/' + str(index)
			metafile.close()
			return baseDatafileName
		except:
			logger.error(traceback.format_exc().split('\n'))

	def incrementIndex(self):
		try:
			baseDirName = TL_DATA_DIR + '/' + self.namespace + '/' + self.source
			metafileName = baseDirName + '/' + '.metafile'
			metafile = open(metafileName, 'r+')
			index = int(metafile.read())
			baseDatafileName = baseDirName + '/' + str(index)
			metafile.seek(0)
			metafile.truncate()
			metafile.write(str(index+1))
			metafile.close()
		except:
			logger.error(traceback.format_exc().split('\n'))


class TLRawData:
	'''
		Writing raw data collected by TL agent to disk
	'''

	def write(self, baseDatafileName, data, compressed=False):

		try:
			if compressed:
				data_sio = StringIO.StringIO(data)
				with gzip.GzipFile(fileobj=data_sio, mode="rb") as f:
					data = f.read()
				f.close()

			datafile = open(baseDatafileName, 'w')
			# adding newline at the end to prevent 'diff' from complaining
			datafile.write(data+'\n') 
			datafile.close()

		except:
			logger.error(traceback.format_exc().split('\n'))

		logger.info("Creating raw file {0}".format(baseDatafileName))


class TLRawDataIndex:
	'''
		Flatten out raw data collected by TL agent and write to disk
		Output file can be used to feed into Solr
	'''

	def write(self, baseDatafileName, data, compressed=False):

		datafileName = baseDatafileName + '.index'

		try:
			if compressed:
				data_sio = StringIO.StringIO(data)
				with gzip.GzipFile(fileobj=data_sio, mode="rb") as f:
					data = f.read()
				f.close()

			files = []
			j = json.loads(data)
			hostname = j['hostname_s']
			collectionTime = j['collection_dt']
			for file in j['files']:
				file['id'] = file['name_s'] + ':' + collectionTime
				file['hostname_s'] = hostname
				file['collection_dt'] = collectionTime
				files.append(file)

			datafile = open(datafileName, 'w')
			json.dump(files, datafile, indent=2)
			# adding newline at the end to prevent 'diff' from complaining
			datafile.write('\n') 
			datafile.close()

		except:
			logger.error(traceback.format_exc().split('\n'))

		logger.info("Creating raw index file {0}".format(datafileName))
		return
