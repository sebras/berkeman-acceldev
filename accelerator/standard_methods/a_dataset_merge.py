############################################################################
#                                                                          #
# Copyright (c) 2019 Carl Drougge                                          #
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

description = r'''
Merge two or more datasets.
The datasets must have the same number of lines in each slice
and if they do not have a common ancestor you must set
allow_unrelated=True.

Columns from later datasets override columns of the same name
from earlier datasets.
'''

from accelerator.dataset import merge_datasets

options = dict(
	allow_unrelated=False,
)
datasets = (['source'], 'previous',)

def synthesis():
	merge_datasets(datasets.source, allow_unrelated=options.allow_unrelated, previous=datasets.previous)