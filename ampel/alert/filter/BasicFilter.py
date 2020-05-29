#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-alerts/ampel/alert/filter/BasicFilter.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.01.2018
# Last Modified Date: 30.01.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import operator
from ampel.abstract.AbsAlertFilter import AbsAlertFilter
from ampel.alert.PhotoAlert import PhotoAlert


class BasicFilter(AbsAlertFilter[PhotoAlert]):

	version = 1.0

	ops = {
		'>': operator.gt,
		'<': operator.lt,
		'>=': operator.ge,
		'<=': operator.le,
		'==': operator.eq,
		'!=': operator.ne
	}

	def __init__(
		self, on_match_t2_units, base_config=None, run_config=None, logger=None
	):

		self.on_match_default_t2_units = on_match_t2_units

		if run_config is None or not isinstance(run_config, dict):
			raise ValueError(f"run_config type must be a dict (got {type(run_config)})")

		self.param = {
			'operator': BasicFilter.ops[run_config['operator']],
			'criteria': run_config['criteria'],
			'len': run_config['len']
		}

		logger.info(f"Following BasicFilter criteria were configured: {self.param}")


	def apply(self, alert):
		""" Doc will follow """

		if self.param['operator'](
			len(
				alert.get_values(
					'candid', filters=self.param['criteria']
				)
			),
			self.param['len']
		):
			return self.on_match_default_t2_units

		return None
