#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-alerts/ampel/alert/FilteringAlertSupplier.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 28.05.2020
# Last Modified Date: 28.05.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from io import IOBase
from typing import List, TypeVar, Dict, Any, Iterable
from ampel.alert.AmpelAlert import AmpelAlert
from ampel.abstract.AbsAlertSupplier import AbsAlertSupplier
from ampel.model.UnitModel import UnitModel
from ampel.base.AuxUnitRegister import AuxUnitRegister

T = TypeVar("T", bound=AmpelAlert)


class FilteringAlertSupplier(AbsAlertSupplier[T]):
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

	supplier: UnitModel
	match_ids: List[int]


	def __init__(self, **kwargs) -> None:
		super().__init__(**kwargs)
		self.underlying_alert_supplier: AbsAlertSupplier[T] = AuxUnitRegister.new_unit(
			unit_model = self.supplier, sub_type = AbsAlertSupplier
		)


	def __next__(self) -> T:
		"""
		:returns: a dict with a structure that AlertProcessor understands
		:raises StopIteration: when alert_loader dries out.
		:raises AttributeError: if alert_loader was not set properly before this method is called
		"""

		nxt = self.underlying_alert_supplier.__next__
		while ret := nxt():
			if ret.id in self.match_ids:
				return ret
		raise StopIteration


	def set_alert_source(self, alert_loader: Iterable[IOBase]) -> None:
		self.underlying_alert_supplier.set_alert_source(alert_loader)


	def ready(self) -> bool:
		return hasattr(self.underlying_alert_supplier, "alert_loader")


	def get_stats(self, reset: bool = True) -> Dict[str, Any]:
		return {}
