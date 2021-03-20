#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-alerts/ampel/abstract/ingest/AbsAlertContentIngester.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 10.03.2020
# Last Modified Date: 18.03.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Sequence, Generic, TypeVar
from ampel.base import abstractmethod
from ampel.alert.AmpelAlert import AmpelAlert
from ampel.content.DataPoint import DataPoint
from ampel.abstract.ingest.AbsIngester import AbsIngester

T = TypeVar("T", bound=AmpelAlert)
V = TypeVar("V", bound=DataPoint)


class AbsAlertContentIngester(Generic[T, V], AbsIngester, abstract=True):
	"""
	:param alert_history_length: alerts must not contain all available info for a given transient.
	IPAC generated alerts for ZTF for example currently provide a photometric history of 30 days.
	Although this number is unlikely to change, there is no reason to use a constant in code.
	"""

	alert_history_length: int


	@abstractmethod
	def ingest(self, alert: T) -> Sequence[V]:
		"""
		:returns: a time-ordered sequence of DataPoints made of the shaped content \
		from the alert together with the content from the DB
		"""
