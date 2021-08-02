#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-alerts/ampel/alert/load/DirAlertLoader.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.12.2017
# Last Modified Date: 27.07.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import List, Optional, Union
from io import BytesIO, StringIO
from ampel.abstract.AbsAlertLoader import AbsAlertLoader


class DirAlertLoader(AbsAlertLoader[Union[StringIO, BytesIO]]):
	""" Load alerts from a (flat) directory. """

	folder: str
	extension: str
	binary_mode: bool = True
	min_index: Optional[int] = None
	max_index: Optional[int] = None
	max_entries: Optional[int] = None


	def __init__(self, **kwargs) -> None:
		super().__init__(**kwargs)
		self.files: List[str] = []
		self.open_mode = "rb" if self.binary_mode else "r"


	def set_extension(self, extension: str) -> None:
		self.extension = extension


	def set_folder(self, arg: str) -> None:
		self.folder = arg
		self.logger.debug("Target incoming folder: " + self.folder)


	def set_index_range(self, min_index: int = None, max_index: int = None) -> None:
		self.min_index = min_index
		self.max_index = max_index
		self.logger.debug(f"Min index set to: {self.min_index}")
		self.logger.debug(f"Max index set to: {self.max_index}")


	def set_max_entries(self, max_entries: int):
		self.max_entries = max_entries
		self.logger.debug(f"Max entries set to: {self.max_entries}")


	def add_files(self, arg: str):
		self.files.append(arg)
		self.logger.debug(f"Adding {len(arg)} files to the list")


	def build_file_list(self) -> None:

		self.logger.debug("Building internal file list")

		import glob, os
		all_files = sorted(
			glob.glob(
				os.path.join(self.folder, self.extension)
			),
			key=os.path.getmtime
		)
		self.logger.debug(f"Building internal file list {len(all_files)}")

		if self.min_index is not None:
			self.logger.debug("Filtering files using min_index criterium")
			out_files = []
			for f in all_files:
				if int(os.path.basename(f).split(".")[0]) >= self.min_index:
					out_files.append(f)
			all_files = out_files

		if self.max_index is not None:
			self.logger.debug("Filtering files using max_index criterium")
			out_files = []
			for f in all_files:
				if int(os.path.basename(f).split(".")[0]) <= self.max_index:
					out_files.append(f)
			all_files = out_files

		if self.max_entries is not None:
			self.logger.debug("Filtering files using max_entries criterium")
			self.files = all_files[:self.max_entries]
		else:
			self.files = all_files

		self.logger.debug(f"File list contains {len(self.files)} elements")


	def __next__(self) -> Union[StringIO, BytesIO]:

		if not self.files:
			self.build_file_list()
			self.iter_files = iter(self.files)

		if (fpath := next(self.iter_files, None)) is None:
			raise StopIteration

		if self.logger.verbose > 1:
			self.logger.debug("Loading " + fpath)

		with open(fpath, self.open_mode) as alert_file:
			return BytesIO(alert_file.read()) if self.binary_mode else StringIO(alert_file.read())
