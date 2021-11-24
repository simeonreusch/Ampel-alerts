#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-alerts/ampel/abstract/AbsAlertFilter.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 24.11.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Optional, Union
from ampel.base.AmpelABC import AmpelABC
from ampel.base.decorator import abstractmethod
from ampel.base.LogicalUnit import LogicalUnit
from ampel.protocol.AmpelAlertProtocol import AmpelAlertProtocol


class AbsAlertFilter(AmpelABC, LogicalUnit, abstract=True):
	""" Base class for T0 alert filters """

	@abstractmethod
	def process(self, alert: AmpelAlertProtocol) -> Optional[Union[bool, int]]:
		"""
		Filters an alert.
		
		:return:
			- None or False: reject the alert
			- True: accept the alert and create all defined t2 documents
			- positive integer greater zero: accept the alert and create t2 documents associated with this group id
			- negative integer: filter (own) rejection code (must not exceed 255)
		"""
