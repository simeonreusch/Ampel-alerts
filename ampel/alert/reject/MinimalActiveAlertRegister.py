#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-alerts/ampel/alert/reject/MinimalActiveAlertRegister.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 12.05.2020
# Last Modified Date: 25.05.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from struct import pack
from typing import Optional, ClassVar, Tuple, Union, Sequence
from ampel.alert.AmpelAlert import AmpelAlert
from ampel.alert.reject.MinimalAlertRegister import MinimalAlertRegister


class MinimalActiveAlertRegister(MinimalAlertRegister):
	""" Logs: alert_id, filter_res. No time stamp. """

	__slots__: ClassVar[Tuple[str, ...]] = '_write', 'alert_max', 'alert_min' # type: ignore[misc]
	_slot_defaults = {'alert_max': 0, 'alert_min': 2**64}

	new_header_size: Union[int, str] = "+300"
	header_hints: ClassVar[Sequence[str]] = ('alert', ) # type: ignore
	alert_min: int
	alert_max: int


	# Override
	def file(self, alert: AmpelAlert, filter_res: Optional[int] = None) -> None:
		alid = alert.id
		if alid > self.alert_max:
			self.alert_max = alid
		if alid < self.alert_min:
			self.alert_min = alid
		self._write(pack('<QB', alid, filter_res or 0))
