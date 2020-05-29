#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-alerts/ampel/model/T0AddModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 10.03.2020
# Last Modified Date: 07.04.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import Field
from typing import List, Optional
from ampel.model.PlainUnitModel import PlainUnitModel
from ampel.model.T1CombineModel import T1CombineModel
from ampel.model.T2ComputeModel import T2ComputeModel

class T0AddModel(PlainUnitModel):
	# Override 'unit' to enable alias
	unit: str = Field(..., alias='ingester')
	# config (datapoint ingester config [from PlainUnitModel])
	t1_combine: Optional[List[T1CombineModel]]
	t2_compute: Optional[T2ComputeModel]
