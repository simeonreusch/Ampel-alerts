#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-alerts/ampel/alert/load/FileAlertLoader.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 30.04.2018
# Last Modified Date: 15.03.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from io import BytesIO
from typing import List, Union, Optional
from ampel.log.AmpelLogger import AmpelLogger
from ampel.base.AmpelBaseModel import AmpelBaseModel


class FileAlertLoader(AmpelBaseModel):
	"""
	Load alerts from one of more files.
	"""

	#: paths to files to load
	files: List[str] = []

	def __init__(self, **kwargs) -> None:
	
		super().__init__(**kwargs)

		if self.files:
			self.add_files(self.files)


	def add_files(self, arg: Union[List[str], str], logger: Optional[AmpelLogger] = None) -> None:

		if isinstance(arg, str):
			arg = [arg]

		for fp in arg:
			if logger:
				logger.info(f"Adding {len(arg)} file(s) to the list")

		self.iter_files = iter(arg)


	def __iter__(self):
		return self


	def __next__(self) -> BytesIO:
		with open(next(self.iter_files), "rb") as alert_file:
			return BytesIO(alert_file.read())
