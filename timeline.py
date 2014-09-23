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

try: import simplejson as json
except ImportError: import json

####################################################################################
# 
# Globals
#
####################################################################################

app = bottle.Bottle()
logger = None

# this string should be same as the contents of the README.API file
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
TL_LOG		=	'./tl.log'
TL_DATA_DIR	=	'./data'

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

	bottle.response.content_type = 'text/plain'

	namespaceInvalid = re.match('^[\w\-\.]+$', namespace) is None
	if namespaceInvalid:
		msg = "Unexpected namespace: {0}".format(namespace)
		logger.warn(msg)
		return msg

	sourceInvalid = re.match('^[\w-]+$', source) is None
	if sourceInvalid:
		msg = "Unexpected source: {0}".format(source)
		logger.warn(msg)
		return msg

	# Handled compressed data
	compressed = False
	if bottle.request.query:
		value = bottle.request.query.get('compressed', None)
		if value:
			if value.lower() == 'true':
				compressed = True
			else:
				compressed = False

	baseDirName = TL_DATA_DIR + '/' + namespace + '/' + source
	metafileName = baseDirName + '/' + '.metafile'

	if not os.path.isdir(baseDirName):
		try:
			logger.info("Creating directory {0}".format(baseDirName))
			os.makedirs(baseDirName)
		except:
			msg = "Cannot create directory {0}".format(baseDirName)
			logger.warn(msg)
			return msg

	if not os.path.isfile(metafileName):
		try:
			logger.info("Creating metafile {0}".format(metafileName))
			metafile = open(metafileName, 'w')
			metafile.write('0')
			metafile.close
		except:
			msg = "Cannot create metafile {0}".format(metafileName)
			logger.warn(msg)
			return msg

	try:
		metafile = open(metafileName, 'r+')
		index = int(metafile.read())

		data = ''
		d = ''
		dataSize = bottle.request.content_length
		if (dataSize < bottle.request.MEMFILE_MAX):
			# bottle stores request body in a string if its size is smaller than
			# MEMFILE_MAX
			data = bottle.request.body.getvalue()
		else:
			# bottle stores request body in a file if its size is greater than
			# MEMFLE_MAX, so we have to treat these separately
			#msg = "POST size is greater than {0}".format(bottle.request.MEMFILE_MAX)
			#logger.warn(msg)
			data = bottle.request.body.read()

		if compressed:
			data_sio = StringIO.StringIO(data)
			with gzip.GzipFile(fileobj=data_sio, mode="rb") as f:
				d = f.read()
			data = d
			f.close()

		datafileName = baseDirName + '/' + str(index)
		datafile = open(datafileName, 'w')
		datafile.write(data+'\n') # adding newline at the end to prevent 'diff' from complaining
		datafile.close()

		metafile.seek(0)
		metafile.truncate()
		metafile.write(str(index+1))
		metafile.close()

		p1 = multiprocessing.Process(target=indexData, args=(datafileName,))
		p1.daemon = True
		p1.start()

		p2 = multiprocessing.Process(target=diffData, args=(datafileName,))
		p2.daemon = True
		p2.start()

	except:
		exc_type, exc_value, exc_traceback = sys.exc_info()
		lines = traceback.format_exception(exc_type, exc_value, exc_traceback)
		msg = ''.join('!! ' + line for line in lines)

		logger.warn(msg)
		return msg

	msg = "file {0} created with length {1}".format(datafileName, dataSize)
	logger.info(msg)

	return msg
	
def indexData(datafileName):
	''' index data, e.g., using Solr '''
	logger.info("Starting a new indexing process({0}) for {1}".format(os.getpid(),datafileName))
	return

def findChanges(file1,file2):

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

	file1.clear()
	file2.clear()

	return result

def diff2JSON(diff):
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
				data = data[:-1]	# remove the trailing ','
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
				step = 3	# this will continue to run the 'if step == 3' code
			else:
				m = linepattern.match(line)
				if m is not None:
					ret = findChanges(file1, file2)
					result.extend(ret)
				else:
					return None

		# step 3: find file2 pattern
		if step == 3:
			m = file2pattern.match(line)
			if m is not None:
				data = m.group(1)
				data = data[:-1]	# remove the trailing ','
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
					ret = findChanges(file1, file2)
					result.extend(ret)
					continue
				else:
					logger.warn('Abort, cannot parse line: {0}'.format(line))
					return None

	# to handle the last diff section in the file
	ret = findChanges(file1, file2)
	result.extend(ret)

	return result

def diffData(datafileName):
	''' find differences between this datafile and the last one '''

	m = re.match(r'(^.*)/([0-9]+$)', datafileName)
	baseDirName = m.group(1)
	index = int(m.group(2))

	if index == 0:
		return	

	for i in range(index-1, -1, -1):
		file = baseDirName + "/" + str(i)
		if os.path.isfile(file):
			logger.info("Starting a new diffing process({0}) for {1} and {2}".format(os.getpid(),datafileName,file))
			try:
				output = subprocess.check_output(['diff', '-w', '-B', file, datafileName])
			except subprocess.CalledProcessError as e:
				# diff will return 1 if files are different
				output = e.output
			finally:
				f = open(baseDirName + '/' + str(i) + '.' + str(index) + '.diff' , 'w')
				f.write(output)
				f.close()

				result = diff2JSON(output)
				f = open(baseDirName + '/' + str(i) + '.' + str(index) + '.json' , 'w')
				json.dump(result, f, indent=4)
				f.close()
			break

	return
	

# Main listen/exec loop

if __name__ == '__main__':
	print ''
	print 'Starting Time Line process({0})'.format(os.getpid())
	print 'Log output will be stored in {0}'.format(TL_LOG)
	print ''
	logging.basicConfig(filename=TL_LOG, filemode='w', format='%(asctime)s %(levelname)s : %(message)s', level=logging.DEBUG)
	logger = logging.getLogger(__name__)
	logger.info('Started Time Line process({0})'.format(os.getpid()))
	logger.info('Log output will be stored in {0}'.format(TL_LOG))
	app.run(host=TL_HOST, port=TL_PORT, quiet=True)
