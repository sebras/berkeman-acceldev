#!/usr/bin/env python2.7

from __future__ import unicode_literals
from glob import glob
from collections import defaultdict
from bottle import route, request, auth_basic, abort
import bottle
from threading import Lock
import json
import re
import time

LOGFILEVERSION = '0'

lock = Lock()

class DotDict(dict):
	"""Like a dict, but with d.foo as well as d['foo'].
	d.foo returns '' for unset values.
	The normal dict.f (get, items, ...) still return the functions.
	"""
	__setattr__ = dict.__setitem__
	__delattr__ = dict.__delitem__
	def __getattr__(self, name):
		if name[0] == "_":
			raise AttributeError(name)
		return self[name]


def joblistlike(jl):
	assert isinstance(jl, list)
	for v in jl:
		assert isinstance(v, (list, tuple)), v
		assert len(v) == 2, v
		for s in v:
			assert isinstance(s, unicode), s
	return True


class DB:
	def __init__(self, path):
		self._initialised = False
		self.path = path
		self.db = defaultdict(dict)
		if os.path.isdir(path):
			files = glob(os.path.join(path, '*/*.urd'))
			print 'init: ', files
			for fn in files:
				with open(fn) as fh:
					for line in fh:
						key, ts, data = self._parse(line)
						self.db[key][ts] = data
		else:
			print "Creating directory \"%s\"." % (path,)
			os.makedirs(path)
		self._initialised = True

	def _parse(self, line):
		line = line.rstrip('\n').split('|')
		logfileversion, _writets = line[:2]
		assert logfileversion == '0'
		key = line[3]
		user, automata = key.split('/')
		data = DotDict(timestamp=line[2],
			user=user,
			automata=automata,
			deps=json.loads(line[4]),
			joblist=json.loads(line[5]),
			caption=line[6],
		)
		self._validate_timestamp(data.timestamp)
		return key, data.timestamp, data

	def _validate_timestamp(self, timestamp):
		assert re.match(r"\d{8}( \d\d(\d\d(\d\d)?)?)?", timestamp), timestamp

	def _validate_data(self, data, with_deps=True):
		if with_deps:
			assert set(data) == {'timestamp', 'joblist', 'caption', 'user', 'automata', 'deps',}
			assert isinstance(data.user, unicode)
			assert isinstance(data.automata, unicode)
			assert isinstance(data.deps, dict)
			for v in data.deps.itervalues():
				assert isinstance(v, dict)
				self._validate_data(DotDict(v), False)
		else:
			assert set(data) == {'timestamp', 'joblist', 'caption',}
		assert joblistlike(data.joblist), data.joblist
		assert data.joblist
		assert isinstance(data.caption, unicode)
		self._validate_timestamp(data.timestamp)

	def _serialise(self, data):
		self._validate_data(data)
		json_deps = json.dumps(data.deps)
		json_joblist = json.dumps(data.joblist)
		now = time.strftime("%Y%m%d %H%M%S")
		for s in json_deps, json_joblist, data.caption, data.user, data.automata, data.timestamp:
			assert '|' not in s, s
		s = '|'.join([LOGFILEVERSION, now, data.timestamp, "%s/%s" % (data.user, data.automata), json_deps, json_joblist, data.caption,])
		print 'serialise', s
		return s

	def add(self, data):
		with lock:
			db = self.db['%s/%s' % (data.user, data.automata)]
			if data.timestamp in db:
				new = False
				changed = (db[data.timestamp] != data)
			else:
				new = True
			if new or changed:
				self.log(data) # validates, too
				db[data.timestamp] = data
			return 'new' if new else 'updated' if changed else 'unchanged'

	def log(self, data):
		if self._initialised:
			assert '/' not in data.user
			assert '/' not in data.automata
			path = os.path.join(self.path, data.user)
			if not os.path.isdir(path):
				os.makedirs(path)
			with open(os.path.join(path, data.automata + '.urd'), 'a') as fh:
				fh.write(self._serialise(data) + '\n')

	def get(self, key, timestamp):
		if key in self.db:
			db = self.db[key]
			return db.get(timestamp)

	def since(self, key, timestamp):
		db = self.db.get(key, {})
		return {k: v for k, v in db.iteritems() if k > timestamp}

	def latest(self, key):
		if key in self.db:
			db = self.db[key]
			return db[max(db)]

	def first(self, key):
		if key in self.db:
			db = self.db[key]
			return db[min(db)]


def auth(user, passphrase):
	return authdict.get(user) == passphrase

@route('/<user>/<automata>/since/<timestamp>')
def since(user, automata, timestamp):
	return db.since(user + '/' + automata, timestamp)

@route('/<user>/<automata>/latest')
def latest(user, automata):
	return db.latest(user + '/' + automata)

@route('/<user>/<automata>/first')
def first(user, automata):
	return db.first(user + '/' + automata)

@route('/<user>/<automata>/<timestamp>')
def single(user, automata, timestamp):
	return db.get(user + '/' + automata, timestamp)


@route('/add', method='POST')
@auth_basic(auth)
def add():
	data = DotDict(request.json or {})
	if data.user != request.auth[0]:
		abort(401, "Error:  user does not match authentication!")
	result = db.add(data)
	return result


#(setq indent-tabs-mode t)

def readauth(filename):
	d = {}
	with open(filename) as fh:
		for line in fh:
			line = line.strip()
			if not line or line.startswith('#'):  continue
			line = line.split(':')
			assert len(line) == 2, "Parse error in \"" + filename + "\" " +  ':'.join(line)
			d[line[0]] = line[1]
	return d


def jsonify(callback):
	def func(*a, **kw):
		res = callback(*a, **kw)
		if isinstance(res, (bottle.BaseResponse, bottle.BottleException)):
			return res
		return json.dumps(res)
	return func


if __name__ == "__main__":
	from argparse import ArgumentParser
	import os.path
	parser = ArgumentParser(description='pelle')
	parser.add_argument('--port', type=int, default=8080, help='server port')
	parser.add_argument('--path', type=str, default='./', help='database directory')
	args = parser.parse_args()
	print '-'*79
	print args
	print
	authdict = readauth(os.path.join(args.path, 'passwd'))
	db = DB(os.path.join(args.path, 'database'))

	bottle.install(jsonify)
	bottle.run(host='localhost', port=args.port, debug=False, reloader=False, quiet=False)
