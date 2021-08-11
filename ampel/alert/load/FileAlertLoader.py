#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-alerts/ampel/alert/load/FileAlertLoader.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 30.04.2018
# Last Modified Date: 11.08.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from io import BytesIO
from typing import List, Optional
from ampel.log.AmpelLogger import AmpelLogger
from ampel.base.AmpelBaseModel import AmpelBaseModel


class FileAlertLoader(AmpelBaseModel):
	"""
	Load alerts from one of more files.
	"""

	#: paths to files to load
	files: List[str]
	logger: Optional[AmpelLogger]

	def __init__(self, **kwargs) -> None:
	
		super().__init__(**kwargs)

		if not self.files:
			raise ValueError("Parameter 'files' cannot be empty")

		if self.logger:
			self.logger.info(f"Registering {len(self.files)} file(s) to load")

		self.iter_files = iter(self.files)

	def __iter__(self):
		return self

	def __next__(self) -> BytesIO:
		with open(next(self.iter_files), "rb") as alert_file:
			return BytesIO(alert_file.read())
