#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-alerts/ampel/alert/load/FileAlertLoader.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 30.04.2018
# Last Modified Date: 19.03.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from io import BytesIO
from typing import List, Union, Optional
from ampel.log.AmpelLogger import AmpelLogger


class FileAlertLoader:
	"""
	Load alerts from one of more files.
	"""

	def __init__(self,
		files: Optional[Union[List[str], str]] = None,
		logger: Optional[AmpelLogger] = None
	) -> None:
		"""
		:param files: paths to files to load
		"""

		self.logger = AmpelLogger.get_logger() if logger is None else logger
		self.files: List[str] = []

		if files:
			self.add_files(files)


	def add_files(self, arg: Union[List[str], str]) -> None:

		if isinstance(arg, str):
			arg = [arg]

		for fp in arg:
			self.files.append(fp)
			self.logger.debug(f"Adding {len(arg)} file(s) to the list")

		self.iter_files = iter(self.files)


	def __iter__(self):
		return self


	def __next__(self) -> BytesIO:
		with open(next(self.iter_files), "rb") as alert_file:
			return BytesIO(alert_file.read())
