#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-alerts/ampel/model/T1CombineModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 10.03.2020
# Last Modified Date: 07.04.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import Field
from typing import Optional
from ampel.model.PlainUnitModel import PlainUnitModel
from ampel.model.T2ComputeModel import T2ComputeModel

class T1CombineModel(PlainUnitModel):
	# Override 'unit' to enable alias
	unit: str = Field(..., alias='ingester')
	t2_compute: Optional[T2ComputeModel]
