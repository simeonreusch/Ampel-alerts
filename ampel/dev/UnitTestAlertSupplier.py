#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-alerts/ampel/alert/UnitTestAlertSupplier.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 28.05.2020
# Last Modified Date: 29.05.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from io import IOBase
from typing import List, TypeVar, Dict, Any, Iterable
from ampel.alert.AmpelAlert import AmpelAlert
from ampel.abstract.AbsAlertSupplier import AbsAlertSupplier
T = TypeVar("T", bound=AmpelAlert)


class UnitTestAlertSupplier(AbsAlertSupplier[T]):
	"""
	See AbsAlertSupplier docstring.
	example:
	UnitLoader.new_aux_unit(
		UnitModel(
			unit="FilteringAlertSupplier",
			config={
				"supplier": {"unit": "ZiAlertSupplier"},
				"match_ids": [770239962715015024]
			}
		)
	)
	"""
	alerts: List[T]


	def __init__(self, **kwargs) -> None:
		super().__init__(**kwargs)
		self.it = iter(self.alerts)


	def set_alert_source(self, alert_loader: Iterable[IOBase]) -> None:
		pass

	def __next__(self):
		return next(self.it)

	# Mandatory implementation
	def __iter__(self):
		return self.it


	def ready(self) -> bool:
		return True if self.alerts else False


	def get_stats(self, reset: bool = True) -> Dict[str, Any]:
		return {}
