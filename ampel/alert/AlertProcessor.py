#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-alerts/ampel/alert/AlertProcessor.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 10.10.2017
# Last Modified Date: 31.01.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import signal
from io import IOBase
from pymongo.errors import PyMongoError
from typing import Sequence, List, Dict, Union, Any, Iterable, Tuple, Optional, Generic

from ampel.type import ChannelId
from ampel.core.AmpelContext import AmpelContext
from ampel.base.AuxUnitRegister import AuxUnitRegister
from ampel.util.mappings import merge_dict
from ampel.util.freeze import recursive_unfreeze
from ampel.db.DBUpdatesBuffer import DBUpdatesBuffer
from ampel.alert.FilterBlocksHandler import FilterBlocksHandler
from ampel.alert.IngestionHandler import IngestionHandler

from ampel.abstract.AbsProcessorUnit import AbsProcessorUnit
from ampel.abstract.AbsAlertSupplier import AbsAlertSupplier, T

from ampel.log import AmpelLogger, LogFlag, DBEventDoc, VERBOSE
from ampel.log.utils import report_exception
from ampel.log.AmpelLoggingError import AmpelLoggingError
from ampel.log.LighterLogRecord import LighterLogRecord

from ampel.model.UnitModel import UnitModel
from ampel.model.AlertProcessorDirective import AlertProcessorDirective
from ampel.alert.AlertProcessorMetrics import stat_alerts, stat_accepted

CONNECTIVITY = 1
INTERRUPTED = 2
TOO_MANY_ERRORS = 3

class AlertProcessor(Generic[T], AbsProcessorUnit):
	"""
	Class handling the processing of alerts (T0 level).
	For each alert, following tasks are performed:

	* Load the alert
	* Filter alert based on the configured T0 filter
	* Ingest alert based on the configured ingester

	:param iter_max: main loop (in method run()) will stop processing alerts when this limit is reached
	:param error_max: main loop (in method run()) will stop processing alerts when this limit is reached
	:param directives:
		mandatory alert processor directives (AlertProcessorDirective). This parameter will
		determine how the underlying FilterBlocksHandler and IngestionHandler instances are set up.
	:param db_log_format: see `ampel.alert.FilterBlocksHandler.FilterBlocksHandler` docstring
	:param supplier: alert supplier, no time explain more currently

	:param log_profile: See AbsProcessorUnit docstring
	:param db_handler_kwargs: See AbsProcessorUnit docstring
	:param base_log_flag: See AbsProcessorUnit docstring
	:param raise_exc: See AbsProcessorUnit docstring (default False)

	Potential update: maybe allow an alternative way of initialization for this class
	through direct input of (possibly customized, that is the point) FilterBlocksHandler and
	IngestionHandler instances rather than through (deserialized) directives.
	"""

	# General options
	iter_max: int = 50000
	error_max: int = 20
	directives: Sequence[AlertProcessorDirective]
	db_log_format: str = "standard"
	single_rej_col: bool = False
	supplier: Optional[Union[AbsAlertSupplier, UnitModel, str]]
	shout: int = LogFlag.SHOUT


	@classmethod
	def from_process(cls, context: AmpelContext, process_name: str, override: Optional[Dict] = None):
		"""
		Convenience method instantiating an AP using the config entry from a given T0 process.
		Example::

			AlertProcessor.from_process(
				context, process_name="VAL_TEST2/T0/ztf_uw_public", override={'publish_stats': []}
			)
		"""
		args = context.get_config().get( # type: ignore
			f"process.{process_name}.processor.config", dict
		)

		if args is None:
			raise ValueError(f"process.{process_name}.processor.config is None")

		if override:
			args = merge_dict(recursive_unfreeze(args), override) # type: ignore

		return cls(context=context, **args)


	def __init__(self, **kwargs) -> None:
		"""
		:raises:
			ValueError if no process can be loaded or if a process is
			associated with an unknown channel
		"""

		if isinstance(kwargs['directives'], dict):
			kwargs['directives'] = (kwargs['directives'], )

		super().__init__(**kwargs)

		self._ampel_db = self.context.get_database()
		logger = AmpelLogger.get_logger(
			console=self.context.config.get(f"logging.{self.log_profile}.console", dict)
		)
		verbose = AmpelLogger.has_verbose_console(self.context, self.log_profile)

		if self.supplier:
			if isinstance(self.supplier, AbsAlertSupplier):
				self.alert_supplier: AbsAlertSupplier[T] = self.supplier
			else:
				if isinstance(self.supplier, str):
					self.supplier = UnitModel(unit=self.supplier)
				self.alert_supplier = AuxUnitRegister.new_unit(
					unit_model = self.supplier, sub_type = AbsAlertSupplier
				)
		else:
			self.alert_supplier = None # type: ignore[assignment]

		if verbose:
			logger.log(VERBOSE, "AlertProcessor setup")

		# Load filter blocks
		self._fbh = FilterBlocksHandler(
			self.context, logger, self.directives, self.process_name, self.db_log_format
		)

		if verbose:

			gather_t2_units = lambda node: [el.unit for el in node.t2_compute.units] \
				if node.t2_compute else []

			for model in self.directives:

				t2_units = []

				if model.t0_add:

					if model.t0_add.t1_combine:
						for el in model.t0_add.t1_combine:
							if el.t2_compute:
								t2_units += gather_t2_units(el)

					if model.t0_add.t2_compute:
						t2_units += gather_t2_units(model.t0_add)

				if model.t1_combine:
					for el in model.t1_combine:
						t2_units += gather_t2_units(el)

				if model.t2_compute:
					t2_units += gather_t2_units(model)

				logger.log(VERBOSE, f"{model.channel} combined on match t2 units: {t2_units}")

		signal.signal(signal.SIGTERM, self.sig_exit)
		signal.signal(signal.SIGINT, self.sig_exit)

		logger.info("AlertProcessor setup completed")


	def sig_exit(self, signum: int, frame) -> None:
		""" Executed when SIGTERM/SIGINT is caught. Stops alert processing in run() """
		self._cancel_run = INTERRUPTED


	def set_iter_max(self, iter_max: int) -> 'AlertProcessor':
		self.iter_max = iter_max
		return self


	def set_supplier(self, alert_supplier: AbsAlertSupplier[T]) -> 'AlertProcessor':
		"""
		Allows to set a custom alert supplier.
		AlertSupplier instances provide AmpelAlert instances
		and need to be sourced by an alert loader instance
		"""
		self.alert_supplier = alert_supplier
		return self


	def set_loader(self, alert_loader: Iterable) -> 'AlertProcessor':
		"""
		Source the current alert suplier with the provided alert loader.
		AlertLoader instances typically provide file-like objects

		:raises ValueError: if self.alert_supplier is None
		"""
		if not self.alert_supplier:
			raise ValueError("Please set alert supplier first")
		self.alert_supplier.set_alert_source(alert_loader)

		return self


	def process_alerts(self, alert_loader: Iterable[IOBase]) -> None:
		"""
		shortcut method to process all alerts from a given loader until its dries out

		:param alert_loader: iterable returning alert payloads
		:raises ValueError: if self.alert_supplier is None
		"""
		if not self.alert_supplier:
			raise ValueError("Please set alert supplier first")
		self.alert_supplier.set_alert_source(alert_loader)
		processed_alerts = self.iter_max
		while processed_alerts == self.iter_max:
			processed_alerts = self.run()

		#self.logger.info("Alert loader dried out")


	def run(self) -> int:
		"""
		Run alert processing using the internal alert_loader/alert_supplier

		:raises: LogFlushingError, PyMongoError
		"""

		# Setup stats
		#############

		stats = {
			"alerts": stat_alerts,
			"accepted": stat_accepted.labels("any"),
		}

		# An AlertSupplier deserializes file-like objects provided by the AlertLoader
		# and returns an AmpelAlert/PhotoAlert
		if not self.alert_supplier or not self.alert_supplier.ready():
			raise ValueError("Alert supplier not set or not sourced")

		run_id = self.new_run_id()

		# Setup logging
		###############

		logger = AmpelLogger.from_profile(
			self.context, self.log_profile, run_id,
			base_flag = LogFlag.T0 | LogFlag.CORE | self.base_log_flag
		)

		if logger.verbose:
			logger.log(VERBOSE, "Pre-run setup")

		# DBLoggingHandler formats, saves and pushes log records into the DB
		if db_logging_handler := logger.get_db_logging_handler():
			db_logging_handler.auto_flush = False

		# Add new doc in the 'events' collection
		event_doc = DBEventDoc(
			self._ampel_db, process_name=self.process_name,
			run_id=run_id, tier=0
		)

		# Collects and executes pymongo.operations in collection Ampel_data
		updates_buffer = DBUpdatesBuffer(
			self._ampel_db, run_id, logger,
			error_callback = self.set_cancel_run,
			catch_signals = False # we do it ourself
		)

		any_filter = any([fb.filter_model for fb in self._fbh.filter_blocks])
		# if bypassing filters, track passing rates at top level
		if not any_filter:
			stats["filter_accepted"] = [
				stat_accepted.labels(channel)
				for channel in self._fbh.chan_names
			]

		# Setup ingesters
		ing_hdlr = IngestionHandler(
			self.context, self.directives, updates_buffer, logger, run_id
		)

		# Loop variables
		iter_max = self.iter_max
		iter_count = 0
		err = 0

		assert self._fbh.chan_names is not None
		reduced_chan_names: Union[str, List[str]] = self._fbh.chan_names[0] \
			if len(self._fbh.chan_names) == 1 else self._fbh.chan_names
		fblocks = self._fbh.filter_blocks

		if any_filter:
			filter_results: List[Tuple[ChannelId, Union[bool, int]]] = []
		else:
			filter_results = [(fb.channel, True) for fb in self._fbh.filter_blocks]

		# Builds set of stock ids for autocomplete, if needed
		self._fbh.ready(logger, run_id, ing_hdlr)

		self._cancel_run = 0

		# Process alerts
		################

		# The extra is just a feedback for the console stream handler
		logger.log(self.shout, "Processing alerts", extra={'r': run_id})

		with updates_buffer.run_in_thread():

			# Iterate over alerts
			for alert in self.alert_supplier:

				if self._cancel_run:

					print("")
					if self._cancel_run == INTERRUPTED:
						logger.info("Interrupting run() procedure")
						print("Interrupting run() procedure")
					else: # updates_buffer requested to stop processing
						print("Abording run() procedure")

					logger.flush()
					self._fbh.done()
					return iter_count

				# Associate upcoming log entries with the current transient id
				stock_id = alert.stock_id
				extra: Dict[str, Any] = {'alert': alert.id}

				if any_filter:

					filter_results = []

					# Loop through filter blocks
					for fblock in fblocks:
						try:
							# Apply filter (returns None/False in case of rejection or True/int in case of match)
							if res := fblock.filter(alert):
								filter_results.append(res)

						# Unrecoverable (logging related) errors
						except (PyMongoError, AmpelLoggingError) as e:
							print("%s: abording run() procedure" % e.__class__.__name__)
							self._report_ap_error(e, logger, run_id, extra=extra)
							raise e

						# Possibly tolerable errors (could be an error from a contributed filter)
						except Exception as e:

							fblock.forward(db_logging_handler, stock=stock_id, extra=extra)
							self._report_ap_error(
								e, logger, run_id, extra={**extra, 'section': 'filter', 'channel': fblock.channel}
							)

							if self.raise_exc:
								raise e
							else:
								if self.error_max:
									err += 1
								if err == self.error_max:
									logger.error("Max number of error reached, breaking alert processing")
									self.set_cancel_run(TOO_MANY_ERRORS)
				else:
					# if bypassing filters, track passing rates at top level
					for counter in stats["filter_accepted"]:
						counter.inc()

				if filter_results:

					stats["accepted"].inc()

					try:
						ing_hdlr.ingest(alert, filter_results)
					except (PyMongoError, AmpelLoggingError) as e:

						print("%s: abording run() procedure" % e.__class__.__name__)
						self._report_ap_error(e, logger, run_id, extra=extra)
						raise e

					except Exception as e:

						self._report_ap_error(
							e, logger, run_id, filter_results,
							extra={**extra, 'section': 'ingest', 'alert': alert.dict()}
						)

						if self.raise_exc:
							raise e
						else:
							if self.error_max:
								err += 1
							if err == self.error_max:
								logger.error("Max number of error reached, breaking alert processing")
								self.set_cancel_run(TOO_MANY_ERRORS)

				else:

					# All channels reject this alert
					# no log entries goes into the main logs collection sinces those are redirected to Ampel_rej.

					# So we add a notification manually. For that, we don't use logger
					# cause rejection messages were alreary logged into the console
					# by the StreamHandler in channel specific RecordBufferingHandler instances.
					# So we address directly db_logging_handler, and for that, we create
					# a LogDocument manually.
					lr = LighterLogRecord(
						logger.name,
						LogFlag.INFO | logger.base_flag
					)
					lr.stock = stock_id
					lr.channel = reduced_chan_names # type: ignore[assignment]
					lr.extra = {
						'alert': alert.id,
						'allout': True,
					}
					if db_logging_handler:
						db_logging_handler.handle(lr)

				iter_count += 1
				stats["alerts"].inc()

				if iter_count == iter_max:
					logger.info("Reached max number of iterations")
					break

				updates_buffer.check_push()
				if db_logging_handler:
					db_logging_handler.check_flush()

		try:
			logger.log(self.shout, "Processing completed")

			# Flush loggers
			logger.flush()
			self._fbh.done()

			event_doc.update(logger)

		except Exception as e:

			# Try to insert doc into trouble collection (raises no exception)
			# Possible exception will be logged out to console in any case
			report_exception(self._ampel_db, logger, exc=e)

		# Return number of processed alerts
		return iter_count


	def set_cancel_run(self, reason: int = CONNECTIVITY) -> None:
		"""
		Cancels current processing of alerts (when DB becomes unresponsive for example).
		"""
		self._cancel_run = CONNECTIVITY


	def _report_ap_error(self,
		arg_e: Exception, logger: AmpelLogger, run_id: Union[int, List[int]],
		filter_results: Optional[List[Tuple[Union[int, str], Union[bool, int]]]] = None,
		extra: Optional[Dict[str, Any]] = None
	) -> None:
		"""
		:param extra: optional extra key/value fields to add to 'trouble' doc
		"""

		info = {}

		if extra:
			for k in extra.keys():
				info[k] = extra[k]

		if filter_results:
			info['channel'] = [el[0] for el in filter_results]

		# Try to insert doc into trouble collection (raises no exception)
		# Possible exception will be logged out to console in any case
		report_exception(self._ampel_db, logger, exc=arg_e, info=info)
