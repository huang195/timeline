#!/usr/local/bin/python

import sys
import os
import re
import traceback
import logging
import getopt

try: import simplejson as json
except ImportError: import json

####################################################################################
# 
# Globals
#
####################################################################################

logger = None
TL_DATA_DIR	=	'./data'
LT_LOG	=	'logs/lt.log'
LT_CONF_DIR	=	'./conf'

tags = []

####################################################################################
#
# Functions
#
####################################################################################

def parseArgs(argv):
	
	namespace = ''
	source = ''

	try:
		opts, args = getopt.getopt(argv,'n:s:',['namespace=','source='])
	except getopt.GetoptError:
		print 'lasertag.py -n <namespace> -s <source>'
		sys.exit(2)

	for opt, arg in opts:
		if opt in ("-n", "--namespace"):
			namespace = arg
		elif opt in ("-s", "--source"):
			source = arg

	if namespace != '' and source != '':
		return (namespace, source)
	else:
		print 'lasertag.py -n <namespace> -s <source>'
		sys.exit(2)


def readTags():
	'''
		Loads configuration files from LT_CONF_DIR
		Config data will be loaded into tags global var
	'''
	files = os.listdir(LT_CONF_DIR);
	pattern = re.compile(r'.*conf$')
	for m in files:
		match = pattern.match(m)
		if match is not None:
			logger.debug('reading {0}'.format(m))
			f = open(LT_CONF_DIR + '/' + m, 'r')
			t = json.load(f)
			f.close()

			p = t['pattern']
			if p[0] == '@':
				# regex pattern captured in a file
				lst = p[1:]
				f = open(LT_CONF_DIR + '/' + lst, 'r')
				patterns = []
				for line in f:
					# regex in file is exact match only
					patterns.append('#:#'+line.strip())
				f.close()
				t['patterns'] = patterns
			else:
				t['patterns'] = [p]
			tags.append(t)

	logger.debug('tags populated')
	logger.debug(json.dumps(tags, indent=2))

def assignTags(namespace, source):
	'''
		Assign tags to files
	'''

	baseDirName = TL_DATA_DIR + '/' + namespace + '/' + source
	f = open(baseDirName + '/.metafile', 'r')
	index = int(f.read())
	f.close()

	if index <= 0:
		return

	for i in range(1, index):
		try:
			f = open(baseDirName + '/' + str(i) + '.json', 'r')
			data = f.read()
			f.close()
			j = json.loads(data)
			for m in j:
				tagFile(m)

			f = open(baseDirName + '/' + str(i) + '.tag.json', 'w')
			json.dump(j, f, indent=2)
			f.close()
		except IOError:
			# if file doesn't exist, we just keep going
			pass
		except:
			print 'stacktrace: ', traceback.format_exc().split('\n')
			continue

def tagFile(file):

	for t in tags:
		patterns = t['patterns']
		tag = t['tag']
		cat = t['cat']

		for pattern in patterns:

			if len(pattern) >= 3 and pattern[0:3] == '#:#':
				# Look for exact match
				p = pattern[3:]
				if file['name_s'] == p:
					if 'tag_s' in file.keys():
						file['tag_s'].append(tag)
						file['cat_s'].append(cat)
					else:
						file['tag_s'] = [tag]
						file['cat_s'] = [cat]
			else:
				# Look for pattern match
				p = re.compile(pattern)
				m = p.match(file['name_s'])
				if m is not None:
					if 'tag_s' in file.keys():
						file['tag_s'].append(tag)
						file['cat_s'].append(cat)
					else:
						file['tag_s'] = [tag]
						file['cat_s'] = [cat]

if __name__ == '__main__':

	namespace, source = parseArgs(sys.argv[1:])
	print 'Starting Laser Tag process({0}) for namespace({1}) and source({2})'.format(os.getpid(),namespace,source)
	print 'Log output will be stored in {0}'.format(LT_LOG)
	logging.basicConfig(filename=LT_LOG, filemode='w', format='%(asctime)s %(levelname)s : %(message)s', level=logging.DEBUG)
	logger = logging.getLogger(__name__)
	logger.info('Started Laser Tag process({0})'.format(os.getpid()))
	logger.info('Log output will be stored in {0}'.format(LT_LOG))
	readTags()
	assignTags(namespace, source)
