#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-alerts/ampel/abstract/AbsAlertSupplier.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 23.04.2018
# Last Modified Date: 09.05.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import json
from io import IOBase
from typing import Iterable, Dict, Callable, Any, Literal, Generic, TypeVar, Union, Iterator
from ampel.base import abstractmethod
from ampel.alert.AmpelAlert import AmpelAlert
from ampel.base.AmpelABC import AmpelABC
from ampel.base.AmpelBaseModel import AmpelBaseModel

T = TypeVar("T", bound=AmpelAlert)

def identity(arg: Dict) -> Dict:
	"""
	Covers the "no deserialization needed" which might occur
	if the underlying alert loader directly returns dicts
	"""
	return arg


class AbsAlertSupplier(Generic[T], AmpelABC, AmpelBaseModel, abstract=True):
	"""
	Iterable class that, for each alert payload provided by the underlying alert_loader,
	returns an :class:`~ampel.alert.AmpelAlert.AmpelAlert` (or subclass such as
	:class:`~ampel.alert.PhotoAlert.PhotoAlert`) instance.

	:param deserialize: if the alert_loader returns bytes/file_like objects,
	  deserialization is required to turn them into dicts.
	  Currently supported built-in deserialization: 'avro' or 'json'.
	  If you need other deserialization:

	  - Either implement the deserialization in your own alert_loader (that will return dicts)
	  - Provide a callable as parameter for `deserialize`
	"""

	deserialize: Union[None, Literal["avro", "json"], Callable[[Any], Dict]] = None # type: ignore

	def __init__(self, **kwargs) -> None:

		AmpelBaseModel.__init__(self, **kwargs) # type: ignore[call-arg]

		if self.deserialize is None:
			self.deserialize = identity # type: ignore[assignment]

		elif self.deserialize == "json":
			self.deserialize = json.load # type: ignore[assignment]

		elif self.deserialize == "avro":

			from fastavro import reader
			def avro_next(arg: IOBase): # noqa: E306
				return reader(arg).next()

			self.deserialize = avro_next # type: ignore[assignment]

		elif callable(self.deserialize):
			pass
		else:
			raise NotImplementedError(
				f"Deserialization '{self.deserialize}' not implemented"
			)


	def set_alert_source(self, alert_loader: Iterable[IOBase]) -> None:
		"""
		:param alert_loader: iterable that returns alerts content
		  as as file-like objects / bytes
		"""
		self.alert_loader = alert_loader


	def ready(self) -> bool:
		return hasattr(self, "alert_loader")


	def __iter__(self) -> Iterator[T]: # type: ignore
		return self


	@abstractmethod
	def __next__(self) -> T:
		"""
		:returns: an AmpelAlert instance
		:raises StopIteration: when alert_loader dries out.
		:raises AttributeError: if alert_loader was not set properly before this method is called
		"""

	@abstractmethod
	def get_stats(self, reset: bool = True) -> Dict[str, Any]:
		...
