#!/usr/bin/env python

############################################################################
#                                                                          #
# Copyright (c) 2020 Anders Berkeman and Carl Drougge                      #
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

from __future__ import division
import argparse
from accelerator.job import Job
from accelerator.dataset import Dataset
from accelerator.dsinfo import quote
from time import time

def main(argv):
	usage = "%(prog)s [options] job [job [...]]"
	parser = argparse.ArgumentParser(prog=argv.pop(0), usage=usage)
	parser.add_argument('-c', '--chain', action='store_true', help='list all datasets in a chain')
	parser.add_argument('-C', '--non_empty_chain', action='store_true', help='list all non-empty datasets in a chain')
	parser.add_argument('-l', '--list', action='store_true', help='list all datasets in a job with number of rows')
	parser.add_argument('-L', '--chainedlist', action='store_true', help='list all datasets in a job with number of chained rows')
	parser.add_argument('-m', '--suppress_minmax', action='store_true', help='do not print min/max column values')
	parser.add_argument('-n', '--suppress_columns', action='store_true', help='do not print columns')
	parser.add_argument('-q', '--suppress_errors', action='store_true', help='silently ignores bad input datasets/jobids')
	parser.add_argument('-s', '--slices', action='store_true', help='list relative number of lines per slice in sorted order')
	parser.add_argument('-S', '--chainedslices', action='store_true', help='same as -s but for full chain')
	parser.add_argument("job", nargs='+')
	args = parser.parse_args(argv)
	args.chain = args.chain or args.non_empty_chain

	print(args)

	for jobid in args.job:
		j = Job(jobid)
		print(dir(j))
		print()
		print('method:    %s' % (quote(j.params.package + '/' + j.params.method),))
		if j.datasets:
			print('datasets:')
			for x in j.datasets:
				print('  ' + quote(Dataset(x).name))
		files = j.files()
		if files:
			print('files:     ' + ', '.join(quote(x) for x in files()))
		if j.params.options or j.params.datasets or j.params.jobs:
			print('parameters:')
			for thing in ('options', 'datasets', 'jobs'):
				if getattr(j.params, thing):
					print('  ' + thing + ':')
					for key, val in getattr(j.params, thing).items():
						print('    %s = %s' % (key, val))
		print('exectime: %f seconds' % (j.post.exectime.total),)
		print('space on disk')

		print('Created %d seconds ago' % (time() - j.params.starttime,))
