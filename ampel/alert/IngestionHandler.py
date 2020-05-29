#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-alerts/ampel/alert/IngestionHandler.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 01.05.2020
# Last Modified Date: 01.05.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from time import time
from typing import Sequence, List, Dict, Union, Iterable, Tuple, Type
from ampel.type import ChannelId
from ampel.core.UnitLoader import PT
from ampel.core.AmpelContext import AmpelContext
from ampel.util.mappings import build_unsafe_dict_id
from ampel.db.DBUpdatesBuffer import DBUpdatesBuffer
from ampel.alert.AmpelAlert import AmpelAlert
from ampel.content.DataPoint import DataPoint
from ampel.abstract.ingest.AbsStockT2Ingester import AbsStockT2Ingester
from ampel.abstract.ingest.AbsCompoundIngester import AbsCompoundIngester
from ampel.abstract.ingest.AbsStateT2Ingester import AbsStateT2Ingester
from ampel.abstract.ingest.AbsAlertContentIngester import AbsAlertContentIngester
from ampel.abstract.ingest.AbsPointT2Ingester import AbsPointT2Ingester
from ampel.abstract.ingest.AbsStockIngester import AbsStockIngester
from ampel.log.AmpelLogger import AmpelLogger
from ampel.log.LogsBufferDict import LogsBufferDict
from ampel.log.LogRecordFlag import LogRecordFlag
from ampel.log.LogUtils import LogUtils
from ampel.model.PlainUnitModel import PlainUnitModel
from ampel.model.AlertProcessingModel import AlertProcessingModel


class IngestionHandler:

	__slots__ = '__dict__', 'updates_buffer', 'stock_ingester', 'datapoint_ingester', \
		'stock_t2_ingesters', 'point_t2_ingesters', 'state_t2_ingesters', \
		'retro_complete', 'ingest_stats', 'log', 'logd'

	stock_ingester: AbsStockIngester
	datapoint_ingester: AbsAlertContentIngester[AmpelAlert, DataPoint]
	stock_t2_ingesters: List[AbsStockT2Ingester]
	point_t2_ingesters: List[AbsPointT2Ingester]
	state_t2_ingesters: Dict[AbsCompoundIngester, List[AbsStateT2Ingester]]
	updates_buffer: DBUpdatesBuffer
	logd: LogsBufferDict
	retro_complete: List[ChannelId]
	ingest_stats: List[float]


	def __init__(self,
		context: AmpelContext,
		directives: Sequence[AlertProcessingModel],
		updates_buffer: DBUpdatesBuffer,
		logger: AmpelLogger,
		run_id: int,
		verbose: int = 0
	):

		self.updates_buffer = updates_buffer
		self.logger = logger
		self.log = logger.log

		# Collects logs accross the different ingesters, every ingester can
		# append values to extra without breaking log aggregation
		self.logd = {'logs': [], 'extra': {}}

		self.ingest_stats = []
		self.retro_complete = []
		self.state_t2_ingesters = {}
		self.point_t2_ingesters = []
		self.stock_t2_ingesters = []

		for directive in directives:
			self.setup_ingesters(
				context, directive, logger, updates_buffer=updates_buffer,
				logd=self.logd, run_id=run_id, verbose=verbose
			)

		if verbose:
			logger.verbose(
				f"Ingesters: datapoint: 1, stock: 1, "
				f"compound: {len(self.state_t2_ingesters)}, "
				f"t2_state: {sum(len(v) for v in self.state_t2_ingesters.values())}, "
				f"t2_point: {len(self.point_t2_ingesters)}, "
				f"t2_stock: {len(self.stock_t2_ingesters)}"
			)


	def setup_ingesters(self,
		context: AmpelContext, directive: AlertProcessingModel,
		logger: AmpelLogger, **kwargs
	) -> None:
		"""
		:param kwargs: are passed to method new_admin_unit from UnitLoader instance
		typically, these are: updates_buffer, logd, run_id
		"""

		t0_add = directive.t0_add

		# An AP can for now only have a unique datapoint ingester
		if not hasattr(self, 'datapoint_ingester'):
			self.datapoint_ingester = context.loader.new_admin_unit(
				model = t0_add,
				context = context,
				sub_type = AbsAlertContentIngester,
				**kwargs
			)

		# An AP can for now only have a unique stock ingester
		if not hasattr(self, 'stock_ingester'):
			self.stock_ingester = context.loader.new_admin_unit(
				model = directive.stock_update,
				context = context,
				sub_type = AbsStockIngester,
				**kwargs
			)

		if t0_add.t1_combine:

			for t1_combine in t0_add.t1_combine:

				# Build compound ingester id
				comp_ingester = self._get_ingester(
					context, t1_combine, AbsCompoundIngester,
					self.state_t2_ingesters, directive, logger, **kwargs
				)
				if comp_ingester not in self.state_t2_ingesters:
					self.state_t2_ingesters[comp_ingester] = []

				# Notify compound ingester that the current channel
				# requires the creation of states/compounds
				comp_ingester.add_channel(directive.channel)

				# State T2s (should be defined along with t1 ingesters usually)
				if state_t2 := t1_combine.t2_compute:

					# Retrieve list of associated t2 ingesters (we allow
					# the definition of multiple different t2 ingesters)
					t2_ingesters = self.state_t2_ingesters[comp_ingester] # type: ignore

					t2_ingester = self._get_ingester(
						context, state_t2, AbsStateT2Ingester,
						t2_ingesters, directive, logger, **kwargs
					)

					if t2_ingester not in t2_ingesters:
						t2_ingesters.append(t2_ingester)

					# Update state ingester internal config
					t2_ingester.add_ingest_models(directive.channel, state_t2.units)

		# DataPoint T2s
		if point_t2 := t0_add.t2_compute:

			point_ingester = self._get_ingester(
				context, point_t2, AbsPointT2Ingester,
				self.point_t2_ingesters, directive, logger, **kwargs
			)

			if point_ingester not in self.point_t2_ingesters:
				self.point_t2_ingesters.append(point_ingester)

			# Update point ingester internal config
			point_ingester.add_ingest_models(directive.channel, point_t2.units)

		# Standalone T1 processes not supported yet
		if directive.t1_combine:
			raise NotImplementedError()

		# Stock T2s
		if directive.t2_compute:

			stock_t2_ingester = self._get_ingester(
				context, directive.t2_compute, AbsStockT2Ingester,
				self.stock_t2_ingesters, directive, logger, **kwargs
			)

			if stock_t2_ingester not in self.stock_t2_ingesters:
				self.stock_t2_ingesters.append(stock_t2_ingester)

			stock_t2_ingester.add_ingest_models(
				directive.channel, directive.t2_compute.units
			)


	def _get_ingester(self,
		context: AmpelContext, model: PlainUnitModel, sub_type: Type[PT],
		it: Iterable, parent_model: AlertProcessingModel, logger: AmpelLogger, **kwargs
	) -> PT:
		"""
		Method used internally to instantiate ingesters.
		If a request for instantiation is made with the same / a similar*
		config than a previous request (contained in the parameter it),
		then the previous ingester instance is returned.
		*: build_unsafe_dict_id sorts lists by default, see its docstring for details

		:raises: ValueError if new ingester instance cannot be created
		"""

		# Identifies ingesters using hashed unit id and config
		md = build_unsafe_dict_id(
			{"unit": model.unit, "config": model.config}, int
		)

		if ingester := next((el for el in it if el.hash == md), None):

			if kwargs.get('verbose', 0) > 1:
				logger.debug(
					f"[{parent_model.channel}] Updating ingester with model {model} (hash={md})"
				)
			elif kwargs.get('verbose'):
				logger.verbose(
					f"[{parent_model.channel}] Updating ingester with id ..{str(md)[-6:]}"
				)

			return ingester

		if kwargs.get('debug', 0) > 1:
			logger.debug(
				f"[{parent_model.channel}] Creating new ingester with model {model} (hash={md})"
			)
		elif kwargs.get('verbose'):
			logger.verbose(
				f"[{parent_model.channel}] Creating new {model.unit} with id ..{str(md)[-6:]}"
			)

		kwargs['hash'] = md

		# Spawn new instance
		ingester = context.loader.new_admin_unit(
			model = model,
			context = context,
			sub_type = sub_type,
			**kwargs
		)

		if ingester is None:
			raise ValueError()

		return ingester


	def ingest(self,
		alert: AmpelAlert,
		filter_results: List[Tuple[ChannelId, Union[bool, int]]]
	) -> None:

		self.updates_buffer._block_autopush = True
		stock_id = alert.stock_id
		ingester_start = time()
		self.logd['extra'] = {'a': alert.id}

		# T0 ingestion
		datapoints = self.datapoint_ingester.ingest(alert) # type: ignore[union-attr]

		# Stock T2 ingestions
		if self.stock_t2_ingesters:
			for stock_t2_ingester in self.stock_t2_ingesters:
				stock_t2_ingester.ingest(stock_id, filter_results)

		# Point T2 ingestions
		if self.point_t2_ingesters:
			for point_ingester in self.point_t2_ingesters:
				point_ingester.ingest(stock_id, datapoints, filter_results)

		# T1 and associated T2 ingestions
		for comp_ingester, t2_ingesters in self.state_t2_ingesters.items():
			comp_blueprint = comp_ingester.ingest(stock_id, datapoints, filter_results)
			if comp_blueprint:
				for state_ingester in t2_ingesters:
					state_ingester.ingest(stock_id, comp_blueprint, filter_results)

		self.stock_ingester.ingest(stock_id, filter_results, {'alert': alert.id})


		if self.retro_complete:

			try:

				# Part 1: Recreate previous alerts
				##################################

				# Build prev_det_sequences.
				# If datapoints looked like this:
				# [{'_id': 12}, {'_id': -11}, {'_id': 10}, {'_id': -8}, {'_id': -7}, {'_id': 6}, {'_id': -4}]
				# prev_det_sequences would be: [
				# 	[{"_id": 10}, {"_id": -8}, {"_id": -7}, {"_id": 6}, {"_id": -4}],
				# 	[{"_id": 6}, {"_id": -4}]
				# ]

				prev_det_sequences: List[Sequence[DataPoint]] = []
				el = datapoints
				while el := alert._prev_det_seq(el): # type: ignore[assignment]
					prev_det_sequences.append(el)

				if prev_det_sequences:

					# Reduce filter_results to the channels requesting 'retro/accept' auto-complete
					filter_results = [el for el in filter_results if el[0] in self.retro_complete]

					# Part 2: Ingest simulated alerts
					#################################

					# For every reconstructed previous alerts contents
					for prev_datapoints in prev_det_sequences:

						# Point T2 ingestions
						if self.point_t2_ingesters:
							for point_ingester in self.point_t2_ingesters:
								point_ingester.ingest(stock_id, prev_datapoints, filter_results)

						# T1 and associated T2 ingestions
						for comp_ingester, t2_ingesters in self.state_t2_ingesters.items():
							comp_blueprint = comp_ingester.ingest(stock_id, prev_datapoints, filter_results)
							if comp_blueprint:
								for state_ingester in t2_ingesters:
									state_ingester.ingest(stock_id, comp_blueprint, filter_results)

						# Update transient journal
						self.stock_ingester.ingest(
							stock_id, filter_results, {'ac': True, 'alert': [alert.id, len(prev_det_sequences)]}
						)

			except Exception as e:
				LogUtils.log_exception(self.logger, e)

			finally:
				self.retro_complete.clear()

		# Log ingester messages
		logd = self.logd
		chans = [el[0] for el in filter_results] if len(filter_results) > 1 else filter_results[0][0]

		if logd['logs']:

			if 'err' in logd:
				del logd['err']
				flag = LogRecordFlag.ERROR
			else:
				flag = LogRecordFlag.INFO

			log = self.log
			extra = logd.get('extra')
			for l in logd['logs']:
				log(flag, l, channel=chans, stock=stock_id, extra=extra)

			logd['logs'] = []
		else:
			self.log(LogRecordFlag.INFO, None, channel=chans, stock=stock_id, extra=logd['extra'])

		self.ingest_stats.append(time() - ingester_start)
		self.updates_buffer._block_autopush = False
