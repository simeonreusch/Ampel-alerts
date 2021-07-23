#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-alerts/ampel/alert/filter/BasicMultiFilter.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.01.2017
# Last Modified Date: 30.07.2020
# Last Modified By  : Jakob van Santen <jakob.van.santen@desy.de>

import operator
from ampel.abstract.AbsAlertFilter import AbsAlertFilter
from ampel.alert.PhotoAlert import PhotoAlert

from ampel.model.StrictModel import StrictModel
from typing import Literal, Sequence, Union, Callable, Dict, Optional
from pydantic import Field, validator

class PhotoAlertQuery(StrictModel):
	"""
	A filter condition suitable for use with AmpelAlert.get_values()
	"""
	_ops: Dict[str,Callable] = {
		'>': operator.gt,
		'<': operator.lt,
		'>=': operator.ge,
		'<=': operator.le,
		'==': operator.eq,
		'!=': operator.ne,
		'AND': operator.and_,
		'OR': operator.or_
	}
	attribute : str = Field(..., description="Name of a light curve field")
	operator: str = Field(..., description="Comparison operator")
	value: float = Field(..., description="Value to compare to")

	@validator('operator')
	def valid_operator(cls, v):
		if not v in cls._ops:
			raise ValueError(f"Unknown operator '{v}'")
		return v

class BasicFilterCondition(StrictModel):
	criteria: Union[Sequence[PhotoAlertQuery], PhotoAlertQuery]
	len: int = Field(..., ge=0)
	operator: str
	logicalConnection: Literal['AND', 'OR'] = 'AND'
	
	@validator('operator', 'logicalConnection')
	def valid_operator(cls, v):
		return PhotoAlertQuery._ops[v]

	@validator('criteria')
	def to_dicts(cls, v):
		"Cast back to dict for use with PhotoAlert.get_values()"
		if isinstance(v, PhotoAlertQuery):
			v = [v]
		return [q.dict() for q in v]

class BasicMultiFilter(AbsAlertFilter[PhotoAlert]):

	version = 1.0

	filters: Sequence[BasicFilterCondition]

	def process(self, alert: PhotoAlert) -> bool:
		"""
		Filter alerts via AmpelAlert.get_values(). Criteria in each condition
		are ANDed together, and conditions can be combined with AND or OR. For
		example, the following configuration selects alerts with at least 4
		detections where `rb>0.8 and fid==1 and mag<18` OR at least 4 detections
		where `magdiff>0.01`::
		    
		    "filters": [
		      {
		        "criteria": [
		          {
		            "attribute": "rb",
		            "value": 0.8,
		            "operator": ">"
		          },
		          {
		            "attribute": "fid",
		            "value": 1,
		            "operator": "=="
		          },
		          {
		            "attribute": "magpsf",
		            "value": 18,
		            "operator": "<"
		          }
		        ],
		        "len": 4,
		        "operator": ">="
		      },
		      {
		        "logicalConnection": "OR",
		        "criteria": [
		          {
		            "attribute": "magdiff",
		            "value": 0.01,
		            "operator": ">"
		          }
		        ],
		        "len": 4
		        "operator": ">="
		      }
		    ]
		"""

		filter_res = []

		for param in self.filters:

			filter_res.append(
				param.operator( # type: ignore[operator]
					len(
						alert.get_values(
							'candid',
							filters = param.criteria # type: ignore[arg-type]
						)
					),
					param.len
				)
			)

		current_res = False

		for i, param in enumerate(filter_res):

			if i == 0:
				current_res = filter_res[i]
			else:
				current_res = self.filters[i].logicalConnection( # type: ignore[misc]
					current_res, filter_res[i]
				)

		return current_res
