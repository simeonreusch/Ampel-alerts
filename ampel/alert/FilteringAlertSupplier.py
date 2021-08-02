#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-alerts/ampel/alert/FilteringAlertSupplier.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 28.05.2020
# Last Modified Date: 29.07.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import List, Iterator, Generic
from ampel.abstract.AbsAlertSupplier import AbsAlertSupplier, T
from ampel.model.UnitModel import UnitModel
from ampel.log.AmpelLogger import AmpelLogger
from ampel.base.AuxUnitRegister import AuxUnitRegister


class FilteringAlertSupplier(Generic[T], AbsAlertSupplier[T]):
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
			model = self.supplier, sub_type = AbsAlertSupplier
		)

	def __iter__(self) -> Iterator[T]:
		"""
		:returns: a dict with a structure that AlertConsumer understands
		:raises StopIteration: when alert_loader dries out.
		:raises AttributeError: if alert_loader was not set properly before this method is called
		"""

		for el in self.underlying_alert_supplier:
			if el.id in self.match_ids:
				yield el

	def set_logger(self, logger: AmpelLogger) -> None:
		self.logger = logger
		self.underlying_alert_supplier.set_logger(logger)
