#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-alerts/ampel/abstract/AbsAlertSupplier.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 23.04.2018
# Last Modified Date: 29.07.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Generic, TypeVar, Iterator
from ampel.log.AmpelLogger import AmpelLogger
from ampel.base.AmpelABC import AmpelABC
from ampel.base.decorator import abstractmethod
from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.alert.AmpelAlert import AmpelAlert

T = TypeVar("T", bound=AmpelAlert)


class AbsAlertSupplier(Generic[T], AmpelABC, AmpelBaseModel, abstract=True):
	"""
	Iterable class that, for each alert payload provided by the underlying alert_loader,
	returns an :class:`~ampel.alert.AmpelAlert.AmpelAlert` (or subclass such as
	:class:`~ampel.alert.PhotoAlert.PhotoAlert`) instance.
	"""

	def __init__(self, **kwargs) -> None:
		super().__init__(**kwargs)
		self.logger: AmpelLogger = AmpelLogger.get_logger()

	def set_logger(self, logger: AmpelLogger) -> None:
		self.logger = logger

	@abstractmethod
	def __iter__(self) -> Iterator[T]:
		...
