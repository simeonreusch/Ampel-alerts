#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-alerts/ampel/abstract/AbsAlertFilter.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 03.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Optional, Union, Generic
from ampel.type import T
from ampel.base import abstractmethod, AmpelABC
from ampel.base.DataUnit import DataUnit


class AbsAlertFilter(Generic[T], AmpelABC, DataUnit, abstract=True):
	""" Base class for T0 alert filters """

	@abstractmethod
	def apply(self, alert: T) -> Optional[Union[bool, int]]:
		"""
		Filters an alert.
		
		:return:
			- None or False: reject the alert
			- True: accept the alert and create all defined t2 documents
			- positive integer: accept the alert and create t2 documents associated with this group id
			- negative integer: filter rejection code (must not exceed 255)
		"""
