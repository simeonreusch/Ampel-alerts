#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-alerts/setup.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 29.01.2020
# Last Modified Date: 29.01.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from setuptools import setup, find_namespace_packages

import version_query

setup(
	name='ampel-alerts',
	version=version_query.predict_version_str(),
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
		"ampel-interface",
		"ampel-core",
		"ampel-photometry",
		"pymongo",
		"pydantic==1.4",
		"fastavro",
	],
	extras_require = {
		"testing": [
			"pytest",
			"pytest-cov",
			"mongomock",
			"coveralls",
		]
	},
)
