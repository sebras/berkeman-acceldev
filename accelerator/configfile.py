############################################################################
#                                                                          #
# Copyright (c) 2017 eBay Inc.                                             #
# Modifications copyright (c) 2019-2020 Anders Berkeman                    #
# Modifications copyright (c) 2019 Carl Drougge                            #
#                                                                          #
# Licensed under the Apache License, Version 2.0 (the "License");          #
# you may not use this file except in compliance with the License.         #
# You may obtain a copy of the License at                                  #
#                                                                          #
#  http://www.apache.org/licenses/LICENSE-2.0                              #
#                                                                          #
# Unless required by applicable law or agreed to in writing, software      #
# distributed under the License is distributed on an "AS IS" BASIS,        #
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. #
# See the License for the specific language governing permissions and      #
# limitations under the License.                                           #
#                                                                          #
############################################################################

from __future__ import print_function
from __future__ import division

import re
import os
from functools import partial

from accelerator.compat import quote_plus, open

from accelerator.extras import DotDict


_re_var = re.compile(r'\$\{([^\}=]*)(?:=([^\}]*))?\}')
def interpolate(s):
	"""Replace ${FOO=BAR} with os.environ.get('FOO', 'BAR')
	(just ${FOO} is of course also supported, but not $FOO)"""
	return _re_var.subn(lambda m: os.environ.get(m.group(1), m.group(2)), s)[0]


def resolve_listen(listen):
	if '/' not in listen and ':' in listen:
		hostname, post = listen.rsplit(':', 1)
		listen = (hostname or 'localhost', int(post),)
		url = 'http://%s:%d' % (hostname, listen[1],)
	else:
		url = None
	return listen, url

def fixup_listen(project_directory, listen, urd=False):
	listen, url = listen
	if not isinstance(listen, tuple):
		if urd and listen == 'local':
			dir = '.socket.dir/urd'
		else:
			dir = listen
		socket = os.path.join(project_directory, dir)
		url = 'unixhttp://' + quote_plus(os.path.realpath(socket))
	return listen, url


def load_config(filename):
	from accelerator.error import UserError

	key = None
	multivalued = {'workdirs', 'method packages', 'interpreters'}
	required = {'slices', 'logfile', 'workdirs', 'method packages'}
	known = {'target workdir', 'listen', 'urd', 'result directory', 'input directory', 'project directory'} | required | multivalued
	cfg = {key: [] for key in multivalued}
	cfg['listen'] = '.socket.dir/daemon', None

	class _E(Exception):
		pass
	def parse_pair(thing, val):
		a = val.split()
		if len(a) != 2 or not a[1].startswith('/'):
			raise _E("Invalid %s specification %r (expected 'name /path')" % (thing, val,))
		return a
	def check_interpreter(val):
		if val[0] == 'DEFAULT':
			raise _E("Don't override DEFAULT interpreter")
		if not os.path.isfile(val[1]):
			raise _E('%r does not exist' % (val,))
	def check_workdirs(val):
		name, path = val
		if name in (v[0] for v in cfg['workdirs']):
			raise _E('Workdir %s redefined' % (name,))
		if path in (v[1] for v in cfg['workdirs']):
			raise _E('Workdir path %r re-used' % (path,))

	parsers = dict(
		slices=int,
		workdirs=partial(parse_pair, 'workdir'),
		interpreters=partial(parse_pair, 'interpreter'),
		listen=resolve_listen,
		urd=resolve_listen,
	)
	checkers = dict(
		interpreter=check_interpreter,
		workdirs=check_workdirs,
	)

	try:
		with open(filename, 'r', encoding='utf-8') as fh:
			for lineno, line in enumerate(fh, 1):
				line = line.split('#', 1)[0].rstrip()
				if not line.strip():
					continue
				if line == line.strip():
					if ':' not in line:
						raise _E('Expected a ":"')
					key, val = line.split(':', 1)
					if key not in known:
						raise _E('Unknown key %r' % (key,))
				else:
					if not key:
						raise _E('First line indented')
					val = line
				val = interpolate(val).strip()
				if val:
					if key in parsers:
						val = parsers[key](val)
					if key in checkers:
						checkers[key](val)
					if key in multivalued:
						cfg[key].append(val)
					else:
						if key in cfg:
							raise _E("%r doesn't take multiple values" % (key,))
						cfg[key] = val
		lineno = None

		missing = set()
		for req in required:
			if not cfg[req]:
				missing.add(req)
		if missing:
			raise _E('Missing required keys %r' % (missing,))

		# Reformat result a bit so the new format doesn't require code changes all over the place.
		rename = {
			'target workdir': 'target_workdir',
			'method packages': 'method_directories',
			'input directory': 'input_directory',
			'result directory': 'result_directory',
			'project directory': 'project_directory',
		}
		res = DotDict({rename.get(k, k): v for k, v in cfg.items()})
		if 'target_workdir' not in res:
			res.target_workdir = res.workdirs[0][0]
		if 'project_directory' not in res:
			res.project_directory = os.path.dirname(filename)
		res.workdirs = dict(res.workdirs)
		if res.target_workdir not in res.workdirs:
			raise _E('target workdir %r not in defined workdirs %r' % (res.target_workdir, set(res.workdirs),))
		res.interpreters = dict(res.interpreters)
		res.listen, res.url = fixup_listen(res.project_directory, res.listen)
		if res.get('urd'):
			res.urd_listen, res.urd = fixup_listen(res.project_directory, res.urd, True)
		else:
			res.urd_listen, res.urd = None, None
	except _E as e:
		if lineno is None:
			prefix = 'Error in %s:\n' % (filename,)
		else:
			prefix = 'Error on line %d of %s:\n' % (lineno, filename,)
		raise UserError(prefix + e.args[0])

	return res
