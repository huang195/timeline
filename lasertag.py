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
exactmatch = {}		# Use files as hash key to speed up lookup process (99% of patterns are these)
regexmatch = []		# Each regex pattern is looked up sequentially

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

			ps = t['pattern']
			for p in ps:
				if p[0] == '@':
					# regex pattern captured in a file
					lst = p[1:]
					f = open(LT_CONF_DIR + '/' + lst, 'r')
					patterns = json.load(f)
					f.close()
					try:
						if t['patterns']:
							t['patterns'].extend(patterns)
					except KeyError:
						t['patterns'] = patterns
							
					#if 'patterns' not in t.keys():
						#t['patterns'] = patterns
					#else:
						#t['patterns'].extend(patterns)

					for m in patterns:
						try:
							if exactmatch[m['name_s']]:
								exactmatch[m['name_s']].append(t)
						except KeyError:
							exactmatch[m['name_s']] = [t]

						# The code below is bad for performance
						#if m['name_s'] not in exactmatch.keys():
							#exactmatch[m['name_s']] = [t]
						#else:
							#exactmatch[m['name_s']].append(t)

				else:
					try:
						if t['patterns']:
							t['patterns'].append({"name_s": p})
					except KeyError:
						t['patterns'] = [ {"name_s": p} ]
							
					#if 'patterns' not in t.keys():
						#t['patterns'] = [ {"name_s": p} ]
					#else:
						#t['patterns'].append({"name_s": p})
					regexmatch.append(t)

			tags.append(t)

	logger.debug('Finished populating tags')
	#logger.debug(json.dumps(tags, indent=2))

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

			count = 0
			data = []	# All data
			meta = []	# All meta data	
			dataH = {}	# All data hashed by 'name_s'
			package = { 'include': [], 'exclude': [] }	# Found packages
			for line in f:
				count += 1
				if count % 2 == 1:
					meta.append(json.loads(line))
				else:
					j = json.loads(line)
					data.append(j)
					dataH[j['name_s']] = j
			f.close()
			for m in data:
				tagFile(m, dataH, package)

			f = open(baseDirName + '/' + str(i) + '.tag.json', 'w')
			for m,d in zip(meta, data):
				f.write(json.dumps(m) + '\n' + json.dumps(d) + '\n')
			f.close()
		except IOError:
			# if file doesn't exist, we just keep going
			pass
		except:
			print 'stacktrace: ', traceback.format_exc().split('\n')
			continue

def tagFile(file, dataH, package):

	try:
		ts = exactmatch[file['name_s']]
		for t in ts:
			patterns = t['patterns']
			tag = t['tag']
			cat = t['cat']

			if cat.lower() == 'package':
				# All package files need to match
				if tag in package['exclude']:
					continue
				elif tag in package['include']:
					pass
				else:
					matching = 1
					for pattern in patterns:
						name = pattern['name_s']
						try:
							d = dataH[name]
							for key,value in pattern.items():
								if key == 'regex':
									continue
								if key in d.keys() and d[key] == value:
									continue
								else:
									matching = 0
									break
						except KeyError:
							package['exclude'].append(tag)
							matching = 0
							break

						if matching == 0:
							break

					if matching == 1:
						package['include'].append(tag)
					else:
						continue

			if 'tag_s' in file.keys():
				# keep values unique
				if tag not in file['tag_s']:
					file['tag_s'].append(tag)
				if cat not in file['cat_s']:
					file['cat_s'].append(cat)
			else:
				file['tag_s'] = [tag]
				file['cat_s'] = [cat]
	except KeyError:
		pass

	for t in regexmatch:
		patterns = t['patterns']
		tag = t['tag']
		cat = t['cat']

		for pattern in patterns:
			# Look for pattern match
			p = re.compile(pattern['name_s'])
			m = p.match(file['name_s'])
			if m is not None:
				if 'tag_s' in file.keys():
					# keep values unique
					if tag not in file['tag_s']:
						file['tag_s'].append(tag)
					if cat not in file['cat_s']:
						file['cat_s'].append(cat)
				else:
					file['tag_s'] = [tag]
					file['cat_s'] = [cat]

'''
	for t in tags:
		patterns = t['patterns']
		tag = t['tag']
		cat = t['cat']

		for pattern in patterns:

			if 'regex' in pattern.keys() and pattern['regex'].lower() == 'false':
				# Look for exact match
				mismatch = 0
				for key,value in pattern.items():
					if key == 'regex':
						continue
					if key in file.keys() and file[key] == value:
						continue
					else:
						mismatch = 1
						break
				if mismatch == 0:
					if 'tag_s' in file.keys():
						# keep values unique
						if tag not in file['tag_s']:
							file['tag_s'].append(tag)
						if cat not in file['cat_s']:
							file['cat_s'].append(cat)
					else:
						file['tag_s'] = [tag]
						file['cat_s'] = [cat]

			else:
				# Look for pattern match
				p = re.compile(pattern['name_s'])
				m = p.match(file['name_s'])
				if m is not None:
					if 'tag_s' in file.keys():
						# keep values unique
						if tag not in file['tag_s']:
							file['tag_s'].append(tag)
						if cat not in file['cat_s']:
							file['cat_s'].append(cat)
					else:
						file['tag_s'] = [tag]
						file['cat_s'] = [cat]
'''

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
