#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-alerts/setup.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 29.01.2020
# Last Modified Date: 29.01.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from setuptools import setup, find_namespace_packages

setup(
	name='ampel-alerts',
	version='0.7',
	packages=find_namespace_packages(),
	package_data = {
		'': ['py.typed'],
		'conf': [
			'*.json', '**/*.json', '**/**/*.json',
			'*.yaml', '**/*.yaml', '**/**/*.yaml',
			'*.yml', '**/*.yml', '**/**/*.yml'
		]
	},
	install_requires = [
		"pymongo",
		"pydantic==1.4",
		"fastavro",
	],
	extras_require = {
		"testing": [
			"pytest",
			"mongomock",
			"coveralls",
		]
	},
)
