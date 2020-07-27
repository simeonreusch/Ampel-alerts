#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-alerts/ampel/alert/reject/BaseAlertRegister.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 16.05.2020
# Last Modified Date: 24.05.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import BinaryIO, Optional, Literal, Dict, Any, List, Union, Generator, Tuple, ClassVar, Sequence
from ampel.type import ChannelId
from ampel.abstract.AbsAlertRegister import AbsAlertRegister
from ampel.util.register import find, reg_iter
from ampel.log import VERBOSE


class BaseAlertRegister(AbsAlertRegister, abstract=True):
	""" # noqa: E101

	See also AmpelRegister docstring

	:param path_channel_folder: if true, a folder whose name equals the parameter `channel`
	will be created under path `path_base`
	:param file_prefix: use $run_id to save file as <run>.bin.gz (whereby <run> equals the constructor parameter run),
	$channel to save file as <channel>.bin.gz or any string to save file as <string>.bin.gz
	:param file_rotate: additionaly to the parent's class ability to rotate files based on the number of blocks,
	this class can also rotate files based on a max number of run_id registers are associated with.
	Both parameters (runs and blocks) can be used together, the criteria fulfilled will trigger a file rotation.
	Note1: file rotation occurs during the opening of registers, meaning that once a register is opened,
	no check is performed (a register can thus grow beyond the defined limits as long as a process keeps it open).
	Note2: the current file rotation number is encoded in the header. If the current rotation number is 10 and
	you move the rotated file to another folder, the next rotation will create ampel_register.bin.gz.11 nonetheless.


	Possible setups:
	----------------

	1) Register files are named after run_id:
	<path>/<channel>/121.register.gz
	<path>/<channel>/122.register.gz
	<path>/<channel>/123.register.gz
	...

	The register header is updated before each file is closed to save min and max alert IDs
	and min and max stock ids. That way, a potential query for a given alert rejection could
	go through all register files, parse only the header and check if min_alert < target_alert < max_alert,
	and will by that not have to go through the file if the condition is not fulfilled.

	Pro:
	- Avoids any potential concurency issue (this setup should be used for re-runs)
	- Fast query
	Cons:
	- Can generate numerous files (whichs should not be a pblm for any modern file system)

	2) register file are named after channel id:
	<path>/AMPEL_CHANNEL1.register.gz
	<path>/AMPEL_CHANNEL2.register.gz
	...

	Log rotate can be performed based on file size (file_rotate='blocks')
	or number of run_ids (file_rotate='runs').

	Pro:
	- Less files
	Cons:
	- beware: re-run should not use this scheme, as concurent updates to a register are not supported!
	"""

	__slots__: ClassVar[Tuple[str, ...]] = '_write', # type: ignore
	_write: Any

	channel: ChannelId
	run_id: int

	path_channel_folder: bool = True # save files in <path_base>/<channel>/<file>
	file_prefix: Union[str, Literal['$run_id', '$channel']] = '$channel' # save file as <run_id|channel|string>.bin.gz

	# Override
	file_rotate: Optional[Dict[Literal['runs', 'blocks'], int]] # type: ignore[assignment]

	header_hints: ClassVar[Optional[Sequence[str]]]


	def __init__(self, **kwargs):

		super().__init__(autoload=False, **kwargs)

		if self.path_base is None:
			self.path_base = self.context.config.get(
				"resource.folder.rejected_alerts", str, raise_exc=True
			)

		if not self.header_extra_base:
			self.header_extra_base = {}

		self.header_extra_base['channel'] = self.channel
		self.header_extra_base['run'] = self.run_id

		if self.file_prefix == "$run_id":
			self.file_prefix = self.run_id
		elif self.file_prefix == "$channel":
			self.file_prefix = self.channel

		if self.path_channel_folder:
			if self.path_extra:
				self.path_extra.append(self.channel)
			else:
				self.path_extra = [self.channel]

		self.load()
		self._write = self._inner_fh.write

		if self.header_hints:
			hdr = self.header['payload']
			for el in self.header_hints:
				for m in ('min', 'max'):
					if el in hdr and m in hdr[el]:
						object.__setattr__(self, f'{el}_{m}', hdr[el][m])


	def check_rotate(self, header: Dict[str, Any]) -> bool:

		if not self.file_rotate:
			return False

		if 'runs' in self.file_rotate: # type: ignore[operator]
			if isinstance(header['run'], list) and len(header['run']) > self.file_rotate['runs']:
				return True

		return super().check_rotate(header)


	def onload_update_header(self) -> None:
		""" hook called by parent class """

		# Update run header field if applicable
		hdr = self.header['payload']

		if isinstance(hdr['run'], int) and self.run_id != hdr['run']:
			hdr['run'] = [hdr['run'], self.run_id]
		elif isinstance(hdr['run'], list) and self.run_id not in hdr['run']:
			hdr['run'].append(self.run_id)
		else:
			return

		if self.verbose:
			self.logger.log(VERBOSE, f"Header updated with run_id {self.run_id}")


	def close(self, **kwargs) -> None: # type: ignore[override]
		"""
		Automatically adds aggregated "header hints" into the header before called super close()
		"""

		if self.header_hints:
			hdr = self.header['payload']
			for el in self.header_hints:
				if getattr(self, el + '_max') == 0:
					continue
				if el not in self.header['payload']:
					hdr[el] = {}
				hdr[el]['max'] = getattr(self, el + '_max')
				hdr[el]['min'] = getattr(self, el + '_min')

		super().close(**kwargs)


	@classmethod
	def iter(cls,
		f: Union[BinaryIO, str], multiplier: int = 100000, verbose: bool = True
	) -> Generator[Tuple[int, ...], None, None]:
		return reg_iter(f, multiplier, verbose)


	@classmethod
	def find_alert(cls,
		f: Union[BinaryIO, str], alert_id: Union[int, List[int]], alert_id_bytes_len: int = 8, **kwargs
	) -> Optional[List[Tuple[int, ...]]]:
		"""
		:param f: file path (str) or file handle (which will not be closed)
		:param kwargs: see method `ampel.util.register.find` docstring.
		Example:
		In []: <Class>.find_alert('/Users/hu/Documents/ZTF/test/aa/aa.bin.gz', alert_id=1242886)
		Out[]: [(1242886, 16)]
		"""
		return find(
			f, match_int=alert_id, int_bytes_len=alert_id_bytes_len,
			offset=0, header_hint='alert', **kwargs
		)


	@classmethod
	def find_stock(cls,
		f: Union[BinaryIO, str], stock_id: Union[int, List[int]],
		stock_offset: int, stock_bytes_len: int = 8, **kwargs
	) -> Optional[List[Tuple[int, ...]]]:
		"""
		:param f: file path (str) or file handle (which will not be closed)
		:param stock_offset: position of the stock values within each block.
		(ex: if the blocks are made of '<QQB' and stock is the second Q,
		then stock_offset should be set to 8)
		:returns: list of rejection info of the alerts matching the provided stock id
		"""
		return find(
			f, match_int=stock_id, int_bytes_len=stock_bytes_len,
			offset=stock_offset, header_hint='stock', **kwargs
		)
