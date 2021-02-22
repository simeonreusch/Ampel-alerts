#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-alerts/ampel/alert/FilterBlock.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 03.05.2018
# Last Modified Date: 11.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from logging import LogRecord
from typing import Any, Union, Optional, Tuple, Dict, Callable, cast
from ampel.type import ChannelId
from ampel.alert.AmpelAlert import AmpelAlert
from ampel.alert.IngestionHandler import IngestionHandler
from ampel.core.AmpelContext import AmpelContext
from ampel.model.AlertProcessorDirective import FilterModel
from ampel.log.AmpelLogger import AmpelLogger, INFO
from ampel.log.handlers.EnclosedChanRecordBufHandler import EnclosedChanRecordBufHandler
from ampel.log.handlers.ChanRecordBufHandler import ChanRecordBufHandler
from ampel.log.LightLogRecord import LightLogRecord
from ampel.log.LogFlag import LogFlag
from ampel.protocol.LoggingHandlerProtocol import LoggingHandlerProtocol
from ampel.abstract.AbsAlertFilter import AbsAlertFilter
from ampel.abstract.AbsAlertRegister import AbsAlertRegister
from ampel.model.AutoStockMatchModel import AutoStockMatchModel
from ampel.alert.AlertProcessorMetrics import (
	stat_accepted,
	stat_rejected,
	stat_autocomplete,
	stat_time,
)

def no_filter(alert: Any) -> bool:
	return True


class FilterBlock:
	"""
	Helper class for AlertProcessor.
	Among other things, it instantiates and references loggers and T0 filters.
	Note: T0 filter units get a dedicated logger which is associated with a
	RecordBufferingHandler instance because we route the produced logs either to the standard logger
	or to the "rejected logger/dumper" depending whether the alert is accepted or not.
	"""

	__slots__ = '__dict__', 'logger', 'channel', 'context', \
		'retro_complete', 'chan_str', \
		'min_log_msg', 'filter_func', 'ac', 'overrule', \
		'bypass', 'update_rej', 'rej_log_handler', 'rej_log_handle', \
		'file', 'log', 'forward', 'buffer', 'buf_hdlr', 'stock_ids'


	def __init__(self,
		context: AmpelContext,
		channel: ChannelId,
		filter_model: Optional[FilterModel],
		stock_match: Optional[AutoStockMatchModel],
		process_name: str,
		logger: AmpelLogger,
		embed: bool = False
	) -> None:

		self._stock_col = context.db.get_collection('stock')
		self.filter_model = filter_model
		self.context = context

		# Channel name (ex: HU_SN or 1)
		self.channel = channel
		self.chan_str = str(self.channel)

		# stats
		self._stat_accepted = stat_accepted.labels(self.chan_str)
		self._stat_rejected = stat_rejected.labels(self.chan_str)
		self._stat_autocomplete = stat_autocomplete.labels(self.chan_str)
		self._stat_time = stat_time.labels(f"filter.{self.chan_str}")

		self.ac = False
		self.retro_complete = False

		if filter_model:

			# Minimal log entry in case filter does not log anything
			self.min_log_msg: Optional[Dict[str, ChannelId]] = {'c': self.channel} if embed else None
			self.overrule = False
			self.bypass = False
			self.update_rej = True

			# Instantiate/get filter class associated with this channel
			logger.info(f"Loading filter: {filter_model.unit_name}")

			self.buf_hdlr = EnclosedChanRecordBufHandler(logger.level, self.channel) if embed \
				else ChanRecordBufHandler(logger.level, self.channel)

			self.unit_instance = context.loader.new_base_unit(
				unit_model = filter_model,
				sub_type = AbsAlertFilter,
				logger = AmpelLogger.get_logger(
					name = "buf_" + self.chan_str,
					base_flag = (getattr(logger, 'base_flag', 0) & ~LogFlag.CORE) | LogFlag.UNIT,
					console = False,
					handlers = [self.buf_hdlr]
				)
			)

			# Clear possibly existing log entries
			# (logged by filter post_init method)
			self.buf_hdlr.buffer = []
			self.forward = self.buf_hdlr.forward # type: ignore
			self.buffer = self.buf_hdlr.buffer

			self.filter_func = self.unit_instance.apply

			if stock_match:
				if stock_match.filter == 'overrule':
					self.overrule = True
				elif stock_match.filter == 'bypass':
					self.bypass = True

				self.retro_complete = stock_match.retro_complete
				self.update_rej = stock_match.update_rej

			self.ac = self.bypass or self.overrule

			self.rej_log_handle: Optional[Callable[[Union[LightLogRecord, LogRecord]], None]] = None
			self.rej_log_handler: Optional[LoggingHandlerProtocol] = None
			self.file: Optional[Callable[[AmpelAlert, Optional[int]], None]] = None
			self.register: Optional[AbsAlertRegister] = None

		else:

			self.filter_func = no_filter


	def filter(self, alert: AmpelAlert) -> Optional[Tuple[ChannelId, Union[int, bool]]]:
		with self._stat_time.time():
			return self._filter(alert)


	def _filter(self, alert: AmpelAlert) -> Optional[Tuple[ChannelId, Union[int, bool]]]:

		stock_id = alert.stock_id

		if self.bypass and stock_id in self.stock_ids:
			return self.channel, True

		# Apply filter (returns None/False in case of rejection or True/int in case of match)
		res = self.filter_func(alert)

		# Filter accepted alert
		if res is not None and res > 0:

			self._stat_accepted.inc()

			# Write log entries to main logger
			# (note: log records already contain chan info)
			if self.buffer:
				self.forward(self.logger, stock=stock_id, extra={'alert': alert.id})

			# Log minimal entry if channel did not log anything
			else:
				if self.min_log_msg: # embed is True
					if isinstance(res, bool):
						self.log(
							INFO,
							self.min_log_msg,
							extra={'a': alert.id, 'stock': stock_id}
						)
					else:
						self.log(
							INFO,
							{'c': self.channel, 'g': res},
							extra={'a': alert.id, 'stock': stock_id}
						)
				else:
					self.log(
						INFO,
						None,
						extra={
							'a': alert.id,
							'stock': stock_id,
							'channel': self.channel
						}
					)

			# self.ac contains all "kinds" of auto-complete
			if self.ac:
				if alert.is_new(): # Just update stock ids for new alerts
					self.stock_ids.add(stock_id)
				elif stock_id not in self.stock_ids:
					self.stock_ids.add(stock_id)
					# "accept" ac requires backward/retro processing of
					# alert content (see further below in the ingestion part)
					if self.retro_complete:
						self.ih.retro_complete.append(self.channel)

			return self.channel, res

		# Filter rejected alert
		else:

			self._stat_rejected.inc()

			# "live" autocomplete requested for this channel
			if self.overrule and stock_id in self.stock_ids:

				extra_ac = {'a': alert.id, 'ac': True, 'stock': stock_id, 'channel': self.channel}

				# Main logger feedback
				self.log(INFO, None, extra=extra_ac)

				# Update count
				self._stat_autocomplete.inc()

				# Rejected alerts notifications can go to rejected log collection
				# even though it was "auto-completed" because it
				# was actually rejected by the filter/channel
				if self.update_rej:

					if self.buffer:
						if self.rej_log_handle:
							# Clears the buffer
							self.forward(self.rej_log_handler, stock=stock_id, extra=extra_ac)
						else:
							self.buffer.clear()

					# Log minimal entry if channel did not log anything
					else:
						if self.rej_log_handle:
							lrec = LightLogRecord(0, 0, None)
							lrec.stock = stock_id
							lrec.extra = extra_ac
							self.rej_log_handle(lrec)

					if self.file:
						self.file(alert, res)

				# Use default t2 units as filter results
				return self.channel, True

			else:

				if self.buffer:

					# Save possibly existing error to 'main' logs
					if self.buf_hdlr.has_error:
						self.forward(
							self.logger, stock=stock_id, extra={'alert': alert.id},
							clear=not self.rej_log_handler
						)

					if self.rej_log_handler:
						# Send rejected logs to dedicated separate logger/handler
						self.forward(self.rej_log_handler, stock=stock_id, extra={'alert': alert.id})

				if self.file:
					self.file(alert, res)

				return None


	def ready(self, logger: AmpelLogger, run_id: int, ingestion_handler: IngestionHandler) -> None:
		"""
		Dependending on the channel settings, this method might:
		- Builds set of transient ids for "auto complete"
		- open an alert register for rejected alerts.
		- instantiate a logging handler for rejected logs
		"""

		self.logger = logger
		self.log = logger.log

		if self.retro_complete:
			self.ih = ingestion_handler

		#if self.auto_accept or self.retro_complete:
		if self.ac:

			# Build set of transient ids for this channel
			self.stock_ids = {
				el['_id'] for el in self._stock_col.find(
					{'channel': self.channel},
					{'_id': 1}
				)
			}

		if self.filter_model and self.filter_model.reject:

			if 'log' in self.filter_model.reject:

				# DBRejectedLogsHandler for example
				self.rej_log_handler = cast(
					LoggingHandlerProtocol,
					self.context.loader.new_admin_unit(
						unit_model = self.filter_model.reject['log'],
						context = self.context,
						channel = self.channel,
						logger = logger
					)
				)

				if not isinstance(self.rej_log_handler, LoggingHandlerProtocol):
					raise ValueError(
						f"Unit must comply with ampel.log.handler.LoggingHandlerProtocol. "
						f"Offending model:\n {self.filter_model.reject['log']}"
					)

				self.rej_log_handler.set_run_id(run_id) # type: ignore
				self.rej_log_handle = self.rej_log_handler.handle

			if 'register' in self.filter_model.reject:

				self.register = self.context.loader.new_admin_unit(
					unit_model = self.filter_model.reject['register'],
					context = self.context,
					sub_type = AbsAlertRegister,
					logger = logger,
					channel = self.channel,
					run_id = run_id
				)

				self.file = self.register.file


	def done(self) -> None:

		self.ih = None # type: ignore[assignment]

		if self.filter_model and self.filter_model.reject:

			if self.rej_log_handler:
				self.rej_log_handler.flush()
				self.rej_log_handler = None

			if self.register:
				self.register.close()
				self.register = None
