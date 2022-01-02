#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-alerts/ampel/alert/BaseAlertSupplier.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                29.07.2021
# Last Modified Date:  24.11.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

import json
from io import IOBase
from typing import Any, Literal
from collections.abc import Callable, Iterator
from ampel.protocol.AmpelAlertProtocol import AmpelAlertProtocol
from ampel.abstract.AbsAlertSupplier import AbsAlertSupplier
from ampel.log.AmpelLogger import AmpelLogger
from ampel.base.decorator import abstractmethod
from ampel.base.AuxUnitRegister import AuxUnitRegister
from ampel.abstract.AbsAlertLoader import AbsAlertLoader
from ampel.model.UnitModel import UnitModel


def identity(arg: dict) -> dict:
	"""
	Covers the "no deserialization needed" case which might occur
	if the underlying alert loader directly returns dicts
	"""
	return arg


class BaseAlertSupplier(AbsAlertSupplier, abstract=True):
	"""
	:param deserialize: if the alert_loader returns bytes/file_like objects,
	  deserialization is required to turn them into dicts.
	  Currently supported built-in deserialization: 'avro' or 'json'.
	  If you need other deserialization:

	  - Either implement the deserialization in your own alert_loader (that will return dicts)
	  - Provide a callable as parameter for `deserialize`
	"""

	#: Unit to use to load alerts
	loader: UnitModel

	# Underlying serialization
	deserialize: None | Literal["avro", "json", "csv"]


	def __init__(self, **kwargs) -> None:

		# Convenience
		if 'loader' in kwargs and isinstance(kwargs['loader'], str):
			kwargs['loader'] = {"unit": kwargs['loader']}

		super().__init__(**kwargs)

		self.alert_loader: AbsAlertLoader[IOBase] = AuxUnitRegister.new_unit(
			model = self.loader, sub_type = AbsAlertLoader
		)

		if self.deserialize is None:
			self._deserialize: Callable[[Any], dict] = identity

		elif self.deserialize == "json":
			self._deserialize = json.load

		elif self.deserialize == "csv":
			from csv import DictReader
			self._deserialize = DictReader # type: ignore

		elif self.deserialize == "avro":

			from fastavro import reader
			def avro_next(arg: IOBase): # noqa: E306
				return reader(arg).next()

			self._deserialize = avro_next
		else:
			raise NotImplementedError(
				f"Deserialization '{self.deserialize}' not implemented"
			)

	def __iter__(self) -> Iterator[AmpelAlertProtocol]:
		return self

	@abstractmethod
	def __next__(self) -> AmpelAlertProtocol:
		...

	def set_logger(self, logger: AmpelLogger) -> None:
		self.logger = logger
		self.alert_loader.set_logger(logger)
