#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-alerts/ampel/alert/reject/FullAlertRegister.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.05.2020
# Last Modified Date: 26.05.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from time import time
from struct import pack
from typing import Optional, Tuple, Literal, Union, BinaryIO, List, ClassVar
from ampel.alert.AmpelAlert import AmpelAlert
from ampel.alert.reject.BaseAlertRegister import BaseAlertRegister


class FullAlertRegister(BaseAlertRegister):
	"""
	Record: alert_id, stock_id, timestamp, filter_res
	"""

	__slots__: ClassVar[Tuple[str, ...]] = '_write', # type: ignore
	struct: Literal['<QQIB'] = '<QQIB' # type: ignore[assignment]


	def file(self, alert: AmpelAlert, filter_res: Optional[int] = None) -> None:
		self._write(pack('<QQIB', alert.id, alert.stock_id, int(time()), filter_res or 0))


	@classmethod
	def find_stock(cls, # type: ignore[override]
		f: Union[BinaryIO, str], stock_id: Union[int, List[int]], **kwargs
	) -> Optional[List[Tuple[int, ...]]]:
		return super().find_stock(f, stock_id=stock_id, offset_in_block=8, **kwargs)
