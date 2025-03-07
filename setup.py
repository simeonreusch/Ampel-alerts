#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-alerts/setup.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                Unspecified
# Last Modified Date:  02.01.2022
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from setuptools import setup, find_namespace_packages

package_data = {
	'conf': [
		'ampel-alerts/*.yaml', 'ampel-alerts/*.yml', 'ampel-alerts/*.json',
		'ampel-alerts/**/*.yaml', 'ampel-alerts/**/*.yml', 'ampel-alerts/**/*.json',
	],
	'ampel.test': ['test-data/*']
}

setup(
    name = 'ampel-alerts',
    version = '0.8.3.alpha-6',
    description = 'Asynchronous and Modular Platform with Execution Layers',
    author = 'Valery Brinnel',
    maintainer = 'Jakob van Santen',
    maintainer_email = 'jakob.van.santen@desy.de',
    url = 'https://ampelproject.github.io',
    packages = find_namespace_packages(),
    package_data = package_data,
    python_requires = '>=3.10,<3.11'
)
