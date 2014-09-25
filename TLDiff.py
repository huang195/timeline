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

class TLDiffData:
	'''
		Writing raw data collected by TL agent to disk
	'''

	def write(self, baseDatafileName):
		''' 
			Find differences between this datafile and the previous one 
		'''

		m = re.match(r'(^.*)/([0-9]+$)', baseDatafileName)
		baseDirName = m.group(1)
		index = int(m.group(2))

		if index == 0:
			return	

		for i in range(index-1, -1, -1):
			file = baseDirName + "/" + str(i)
			if os.path.isfile(file):
				logger.info("Starting a new diffing process({0}) for {1} and {2}".format(os.getpid(),baseDatafileName,file))
				try:
					output = subprocess.check_output(['diff', '-w', '-B', file, baseDatafileName])
				except subprocess.CalledProcessError as e:
					# diff will return 1 if files are different
					output = e.output
				finally:
					f = open(baseDirName + '/' + str(index) + '.diff' , 'w')
					f.write(output)
					f.close()
				break

		return

class TLDiffDataIndex:
	'''
		Format diff data so it is indexable by Solr
	'''

	def write(self, baseDatafileName):

		m = re.match(r'(^.*)/([0-9]+$)', baseDatafileName)
		baseDirName = m.group(1)
		index = int(m.group(2))

		if index == 0:
			return

		f = open(baseDatafileName + '.diff', "r")
		data = f.read()
		f.close()

		result = self.diff2JSON(data)
		f = open(baseDatafileName + '.json', 'w')
		json.dump(result, f, indent=4)
		f.close()


	def findChanges(self,file1,file2):

		result = []

		for key,value in file1.items():
			if key in file2:
				j = json.loads(file2[key])
				j['change_s'] = 'modified'
				result.append(j)
				del file2[key]
			else:
				j = json.loads(file1[key])
				j['change_s'] = 'deleted'
				result.append(j)

		for key,value in file2.items():
			j = json.loads(file2[key])
			j['change_s'] = 'added'
			result.append(j)

		# Massage for Banana
		for j in result:
			j['id'] = j['name_s'] + ':' + j['lastmodifiedtime_dt']
			j['event_timestamp'] = j['lastmodifiedtime_dt']
			j['message'] = j['change_s'] + ' ' + j['name_s']

		file1.clear()
		file2.clear()

		return result

	def diff2JSON(self, diff):
		''' given a diff output, return it in JSON format

			diff format:
				[0-9]+(,[0-9]+)?c[0-9]+(,[0-9]+)?
				<\s+.*
				---
				>\s+.*
		'''

		result = []
		file1 = {}
		file2 = {}
		step = 1

		linepattern = re.compile(r'[0-9]+(,[0-9]+)?[a-zA-Z][0-9]+(,[0-9]+)?')
		file1pattern = re.compile(r'^\<\s+(.*)$')
		dividerpattern = re.compile(r'^---$')
		file2pattern = re.compile(r'^\>\s+(.*)$')

		for line in diff.split('\n'):

			# handle eof
			if len(line) == 0:	
				break

			# step 1: find line pattern
			if step == 1:
				m = linepattern.match(line)
				if m is not None:
					step = 2
					continue
				else:
					return None

			# step 2: find file1 pattern
			if step == 2:
				m = file1pattern.match(line)
				if m is not None:
					data = m.group(1)
					if data[-1] == ',':
						# remove the trailing ','
						data = data[:-1]
					try:
						dj = json.loads(data)
						file1[dj['name_s']] = data
					except json.JSONDecodeError:
						# if a line is not in json format, we skip it
						logger.warn('line is not in JSON format: {0}'.format(line))

					continue

				m = dividerpattern.match(line)
				if m is not None:
					step = 3
					continue

				m = file2pattern.match(line)
				if m is not None:
					# this will continue to run the 'if step == 3' code
					step = 3
				else:
					m = linepattern.match(line)
					if m is not None:
						ret = self.findChanges(file1, file2)
						result.extend(ret)
					else:
						return None

			# step 3: find file2 pattern
			if step == 3:
				m = file2pattern.match(line)
				if m is not None:
					data = m.group(1)
					if data[-1] == ',':
						# remove the trailing ','
						data = data[:-1]
					try:
						dj = json.loads(data)
						file2[dj['name_s']] = data
					except json.JSONDecodeError:
						# if a line is not in json format, we skip it
						logger.warn('line is not in JSON format: {0}'.format(line))
				
					continue
				else:
					m = linepattern.match(line)
					if m is not None:
						step = 2
						ret = self.findChanges(file1, file2)
						result.extend(ret)
						continue
					else:
						logger.warn('Abort, cannot parse line: {0}'.format(line))
						return None

		# to handle the last diff section in the file
		ret = self.findChanges(file1, file2)
		result.extend(ret)

		return result

