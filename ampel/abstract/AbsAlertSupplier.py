#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File:                Ampel-alerts/ampel/abstract/AbsAlertSupplier.py
# License:             BSD-3-Clause
# Author:              valery brinnel <firstname.lastname@gmail.com>
# Date:                23.04.2018
# Last Modified Date:  24.11.2021
# Last Modified By:    valery brinnel <firstname.lastname@gmail.com>

from typing import Iterator
from ampel.log.AmpelLogger import AmpelLogger
from ampel.base.AmpelABC import AmpelABC
from ampel.base.decorator import abstractmethod
from ampel.base.AmpelBaseModel import AmpelBaseModel
from ampel.protocol.AmpelAlertProtocol import AmpelAlertProtocol


class AbsAlertSupplier(AmpelABC, AmpelBaseModel, abstract=True):
	"""
	Iterable class that, for each alert payload provided by the underlying alert_loader,
	returns an object that implements :class:`~ampel.protocol.AmpelAlertProtocol`.
	"""

	def __init__(self, **kwargs) -> None:
		super().__init__(**kwargs)
		self.logger: AmpelLogger = AmpelLogger.get_logger()


	def set_logger(self, logger: AmpelLogger) -> None:
		self.logger = logger


	@abstractmethod
	def __iter__(self) -> Iterator[AmpelAlertProtocol]:
		...
