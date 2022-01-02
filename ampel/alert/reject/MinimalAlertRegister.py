#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-alerts/ampel/alert/reject/MinimalAlertRegister.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                12.05.2020
# Last Modified Date:  24.11.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from struct import pack
from typing import ClassVar, Literal
from ampel.protocol.AmpelAlertProtocol import AmpelAlertProtocol
from ampel.alert.reject.BaseAlertRegister import BaseAlertRegister


class MinimalAlertRegister(BaseAlertRegister):
	"""
	Logs: alert_id, filter_res. No time stamp.

	Notes:
	- method "iter" yields tuple[<alert id>, <filter return code>]
	"""

	__slots__: ClassVar[tuple[str, ...]] = '_write', # type: ignore
	struct: Literal['<QB'] = '<QB'
	header_log_accesses: bool = False


	def file(self, alert: AmpelAlertProtocol, filter_res: None | int = None) -> None:
		self._write(pack('<QB', alert.id, filter_res or 0))


	@classmethod
	def find_stock(cls) -> None: # type: ignore[override]
		raise NotImplementedError("Minimal registers do not save stock information")
