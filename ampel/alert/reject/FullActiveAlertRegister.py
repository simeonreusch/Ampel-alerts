#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-alerts/ampel/alert/reject/FullActiveAlertRegister.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.05.2020
# Last Modified Date: 26.05.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from time import time
from struct import pack
from typing import Optional, ClassVar, Tuple, Sequence
from ampel.alert.AmpelAlert import AmpelAlert
from ampel.alert.reject.FullAlertRegister import FullAlertRegister


class FullActiveAlertRegister(FullAlertRegister):
	""" Logs: alert_id, stock_id, timestamp, filter_res """

	__slots__: ClassVar[Tuple[str, ...]] = '_write', 'alert_max', 'alert_min', 'stock_max', 'stock_min' # type: ignore
	_slot_defaults = {'alert_max': 0, 'alert_min': 2**64, 'stock_max': 0, 'stock_min': 2**64}

	header_hints: ClassVar[Sequence[str]] = ('alert', 'stock') # type: ignore
	alert_min: int
	alert_max: int
	stock_min: int
	stock_max: int


	def file(self, alert: AmpelAlert, filter_res: Optional[int] = None) -> None:

		alid = alert.id
		if alid > self.alert_max:
			self.alert_max = alid
		if alid < self.alert_min:
			self.alert_min = alid

		sid = alert.stock_id
		if sid > self.stock_max: # type: ignore[operator]
			self.stock_max = sid # type: ignore[assignment]
		if sid < self.stock_min: # type: ignore[operator]
			self.stock_min = sid # type: ignore[assignment]

		self._write(pack('<QQIB', alid, sid, int(time()), filter_res or 0))
