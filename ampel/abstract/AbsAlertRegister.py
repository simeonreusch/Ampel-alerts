#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-alerts/ampel/abstract/AbsAlertRegister.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                09.05.2020
# Last Modified Date:  24.11.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from ampel.base.AmpelABC import AmpelABC
from ampel.base.decorator import abstractmethod
from ampel.core.ContextUnit import ContextUnit
from ampel.core.AmpelRegister import AmpelRegister
from ampel.protocol.AmpelAlertProtocol import AmpelAlertProtocol


class AbsAlertRegister(AmpelABC, AmpelRegister, ContextUnit, abstract=True):
	"""
	Record of the results of filter evaluation, in particular for rejected alerts.
	"""

	@abstractmethod
	def file(self, alert: AmpelAlertProtocol, filter_res: None | int = None) -> None:
		"""
		Record the result of the filter.

		:param alert: the alert a filter was applied to
		:param filter_res: result of the filter; ``None`` if the alert was rejected
		"""
		...
