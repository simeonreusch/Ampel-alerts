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
	Ensemble of classes used mainly for saving information regarding rejected alerts
	"""

	@abstractmethod
	def file(self, alert: AmpelAlert, filter_res: Optional[int] = None) -> None:
		...
