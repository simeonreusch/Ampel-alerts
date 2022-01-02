#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-alerts/ampel/alert/reject/FullAlertRegister.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                14.05.2020
# Last Modified Date:  26.05.2020
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from time import time
from struct import pack
from typing import Literal, BinaryIO, ClassVar
from ampel.protocol.AmpelAlertProtocol import AmpelAlertProtocol
from ampel.alert.reject.BaseAlertRegister import BaseAlertRegister


class FullAlertRegister(BaseAlertRegister):
	"""
	Record: alert_id, stock_id, timestamp, filter_res
	"""

	__slots__: ClassVar[tuple[str, ...]] = '_write', # type: ignore
	struct: Literal['<QQIB'] = '<QQIB' # type: ignore[assignment]


	def file(self, alert: AmpelAlertProtocol, filter_res: None | int = None) -> None:
		self._write(pack('<QQIB', alert.id, alert.stock, int(time()), filter_res or 0))


	@classmethod
	def find_stock(cls, # type: ignore[override]
		f: BinaryIO | str, stock_id: int | list[int], **kwargs
	) -> None | list[tuple[int, ...]]:
		return super().find_stock(f, stock_id=stock_id, offset_in_block=8, **kwargs)
