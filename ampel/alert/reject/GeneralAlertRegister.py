#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-alerts/ampel/alert/reject/GeneralAlertRegister.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 26.05.2020
# Last Modified Date: 26.05.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from struct import pack
from typing import Optional, Tuple, Literal, Union, BinaryIO, List, ClassVar
from ampel.alert.AmpelAlert import AmpelAlert
from ampel.alert.reject.BaseAlertRegister import BaseAlertRegister


class GeneralAlertRegister(BaseAlertRegister):
	""" Logs: alert_id, stock_id, filter_res """

	__slots__: ClassVar[Tuple[str, ...]] = '_write', # type: ignore
	struct: Literal['<QQB'] = '<QQB'


	def file(self, alert: AmpelAlert, filter_res: Optional[int] = None) -> None:
		self._write(pack('<QQB', alert.id, alert.stock_id, filter_res or 0))


	@classmethod
	def find_stock(cls, # type: ignore[override]
		f: Union[BinaryIO, str], stock_id: Union[int, List[int]], **kwargs
	) -> Optional[List[Tuple[int, ...]]]:
		return super().find_stock(f, stock_id=stock_id, stock_offset=8, **kwargs)
