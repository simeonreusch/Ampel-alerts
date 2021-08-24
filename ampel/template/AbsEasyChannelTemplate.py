#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-alerts/ampel/template/AbsEasyChannelTemplate.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 16.10.2019
# Last Modified Date: 16.07.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

import ujson
from pydantic import validator
from typing import List, Dict, Any, Union, Optional
from ampel.types import ChannelId
from ampel.log.AmpelLogger import AmpelLogger
from ampel.model.ingest.T2Compute import T2Compute
from ampel.model.ingest.FilterModel import FilterModel
from ampel.config.builder.FirstPassConfig import FirstPassConfig
from ampel.abstract.AbsChannelTemplate import AbsChannelTemplate
from ampel.util.template import filter_units, resolve_shortcut, check_tied_units


class AbsEasyChannelTemplate(AbsChannelTemplate, abstract=True):
	"""
	Abstract class whose purpose is to maintain compatibility with channel
	definitions created for ampel versions < 0.7.
	This class must be subclassed.
	
	Known subclass: :class:`~ampel.model.ZTFLegacyChannelTemplate.ZTFLegacyChannelTemplate`
	"""
	#: Filter to apply to incoming datapoints
	t0_filter: FilterModel

	#: T2 units to trigger when transient is updated. Dependencies of tied
	#: units will be added automatically.
	t2_compute: List[T2Compute] = []

	#: T3 processes bound to this channel. These may be use templates, such as
	#: :class:`~ampel.model.template.PeriodicSummaryT3.PeriodicSummaryT3`.
	t3_supervise: List[Dict[str, Any]] = []

	retro_complete: bool = False


	@validator('t3_supervise', 't2_compute', pre=True, each_item=False)
	def cast_to_list_if_required(cls, v):
		if isinstance(v, dict):
			return [v]
		return v


	# Mandatory implementation
	def get_channel(self, logger: AmpelLogger) -> Dict[str, Any]:
		d = self.dict(by_alias=True)
		for k in ("t0_filter", "t0_muxer", "t2_compute", "t3_supervise", "auto_complete"):
			if k in d:
				del d[k]
		return d


	def craft_t0_process(self,
		config: Union[FirstPassConfig, Dict[str, Any]],
		controller: Union[str, Dict[str, Any]],
		supplier: Union[str, Dict[str, Any]],
		shaper: Union[str, Dict[str, Any]],
		combiner: Union[str, Dict[str, Any]],
		muxer: Optional[Union[str, Dict[str, Any]]] = None,
		compiler_opts: Optional[Dict[str, Any]] = None
	) -> Dict[str, Any]:
		"""
		This method needs a reference to a FirstPassConfig dict because
		config information might be needed during the template transforming process.
		For example, legacy channel templates (such as ZTFLegacyChannelTemplate)
		allow users to reference any kind of t2 units under the root config section 't2_compute'.
		The AlertConsumer however, requires different configuration paths for "state T2s" and "point T2s".
		The underlying templates will thus have to sort T2s based on their respective abstract classes,
		and for this, the ampel configuration is required.

		:param stock_ingester: unit_class or (unit_class, config dict)
		:param point_t2: units to schedule on t0_add
		:param state_t2: units to schedule on t1_combine
		"""

		ret: Dict[str, Any] = {
			"tier": 0,
			"schedule": ["super"],
			"active": self.active,
			"distrib": self.distrib,
			"source": self.source,
			"channel": self.channel,
			"name": f"{self.channel}|T0|{self.template}",
			"controller": resolve_shortcut(controller),
			"processor": {
				"unit": "AlertConsumer",
				"config": self.craft_t0_processor_config(
					self.channel, config, self.t2_compute, supplier, shaper, combiner,
					self.t0_filter.dict(exclude_unset=True, by_alias=True), muxer, compiler_opts
				)
			}
		}

		return ret


	@classmethod
	def craft_t0_processor_config(cls,
		channel: ChannelId,
		config: Union[FirstPassConfig, Dict[str, Any]],
		t2_compute: List[T2Compute],
		supplier: Union[str, Dict[str, Any]],
		shaper: Union[str, Dict[str, Any]],
		combiner: Union[str, Dict[str, Any]],
		filter_dict: Optional[Dict[str, Any]] = None,
		muxer: Optional[Union[str, Dict[str, Any]]] = None,
		compiler_opts: Optional[Dict[str, Any]] = None
	) -> Dict[str, Any]:
		"""
		This method needs a reference to a FirstPassConfig dict because
		config information might be needed during the template transforming process.
		For example, legacy channel templates (such as ZTFLegacyChannelTemplate)
		allow users to reference any kind of t2 units under the root config section 't2_compute'.
		The AlertConsumer however, requires different configuration paths for "state T2s" and "point T2s".
		The underlying templates will thus have to sort T2s based on their respective abstract classes,
		and for this, the ampel configuration is required.

		:param stock_ingester: unit_class or (unit_class, config dict)
		:param point_t2: units to schedule on t0_add
		:param state_t2: units to schedule on t1_combine
		"""

		state_t2s = filter_units(
			t2_compute, [
				"AbsStateT2Unit",
				"AbsCustomStateT2Unit",
				"AbsTiedStateT2Unit",
				"AbsTiedCustomStateT2Unit",
			],
			config
		)

		stock_t2s = filter_units(t2_compute, "AbsStockT2Unit", config)
		point_t2s = filter_units(t2_compute, "AbsPointT2Unit", config)
		check_tied_units(t2_compute, config)

		ingest: Dict[str, Any] = {}

		# See IngestDirective docstring
		if stock_t2s:
			ingest['stock_t2'] = stock_t2s

		# This template does not support 'free' point t2s (based on input dps list)
		# but anchors potentially available point t2s under 'combine' (based on dps list returned by combine)
		if muxer:
			ingest['mux'] = ujson.loads(ujson.dumps(resolve_shortcut(muxer)))
			if state_t2s:
				ingest['mux']['combine'] = [{'unit': combiner, 'state_t2': state_t2s}]
			if point_t2s:
				ingest['mux']['insert'] = {"point_t2": point_t2s}
		else:
			if state_t2s:
				ingest['combine'] = [{'unit': combiner, 'state_t2': state_t2s}]
			if point_t2s:
				if 'combine' in ingest:
					ingest['combine'][0]['point_t2'] = point_t2s
				else:
					ingest['combine'] = [{'unit': combiner, 'point_t2': point_t2s}]

		return {
			"supplier": resolve_shortcut(supplier),
			"shaper": resolve_shortcut(shaper),
			"compiler_opts": compiler_opts,
			"directives": [
				{"channel": channel, "filter": filter_dict, "ingest": ingest} if filter_dict \
				else {"channel": channel, "ingest": ingest}
			]
		}
