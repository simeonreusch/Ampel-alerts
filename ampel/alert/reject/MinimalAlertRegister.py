#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-alerts/ampel/alert/reject/MinimalAlertRegister.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 12.05.2020
# Last Modified Date: 23.05.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from struct import pack
from typing import Optional, ClassVar, Tuple, Literal
from ampel.alert.AmpelAlert import AmpelAlert
from ampel.alert.reject.BaseAlertRegister import BaseAlertRegister


class MinimalAlertRegister(BaseAlertRegister):
	"""
	Logs: alert_id, filter_res. No time stamp.

	Notes:
	- method "iter" yields Tuple[<alert id>, <filter return code>]
	"""

	__slots__: ClassVar[Tuple[str, ...]] = '_write', # type: ignore
	struct: Literal['<QB'] = '<QB'
	header_log_accesses: bool = False


	def file(self, alert: AmpelAlert, filter_res: Optional[int] = None) -> None:
		self._write(pack('<QB', alert.id, filter_res or 0))


	@classmethod
	def find_stock(cls) -> None: # type: ignore[override]
		raise NotImplementedError("Minimal registers do not save stock information")
