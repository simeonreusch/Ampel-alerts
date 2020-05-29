#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-alerts/ampel/abstract/AbsAlertFilter.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 28.04.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Optional, Union, TypedDict, Any, Dict, Generic, Iterable
from ampel.type import T
from ampel.base import abstractmethod, defaultmethod
from ampel.abstract.AbsDataUnit import AbsDataUnit


class AbsAlertFilter(Generic[T], AbsDataUnit, abstract=True):
	""" Base class for T0 alert filters """

	class T2IngestionPrecept(TypedDict):
		unit: str
		config: Dict[str, Any]
		ingest: Optional[Dict[str, Any]]
		group: Optional[Union[int, Iterable[int]]]


	@defaultmethod(check_super_call=True)
	def __init__(self, **kwargs) -> None:
		AbsDataUnit.__init__(self, **kwargs)
		# subclasses can use post_init as __init__ replacement
		self.post_init()


	@defaultmethod
	def post_init(self) -> None:
		"""
		Defined as default method to ensure that the
		overridding method defines post_init without argument
		(option "force" is not used since we do not add any implementation here)
		"""


	@abstractmethod
	def apply(self, alert: T) -> Optional[Union[bool, int]]:
		"""
		Filters an alert.
		:returns:
		- None or False: reject the alert
		- True: accept the alert and create all defined t2 documents
		- positive integer: accept the alert and create t2 documents associated with this group id
		- negative integer: filter rejection code (must not exceed 255)
		"""
