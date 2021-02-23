#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-alerts/ampel/abstract/AbsAlertRegister.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 09.05.2020
# Last Modified Date: 26.05.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Optional
from ampel.base import abstractmethod
from ampel.core.AdminUnit import AdminUnit
from ampel.alert.AmpelAlert import AmpelAlert
from ampel.core.AmpelRegister import AmpelRegister


class AbsAlertRegister(AmpelRegister, AdminUnit, abstract=True):
	"""
	Record of the results of filter evaluation, in particular for rejected alerts.
	"""

	@abstractmethod
	def file(self, alert: AmpelAlert, filter_res: Optional[int] = None) -> None:
		"""
		Record the result of the filter.

		:param alert: the alert a filter was applied to
		:param filter_res: result of the filter; ``None`` if the alert was rejected
		"""
		...
