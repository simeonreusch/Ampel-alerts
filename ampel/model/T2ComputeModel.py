#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-alerts/ampel/model/T2ComputeModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 10.03.2020
# Last Modified Date: 07.04.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from pydantic import Field
from typing import Sequence
from ampel.model.T2IngestModel import T2IngestModel
from ampel.model.PlainUnitModel import PlainUnitModel


class T2ComputeModel(PlainUnitModel):
	# Override of 'unit' to enable alias
	unit: str = Field(..., alias='ingester')
	# config (t2 ingester config [from PlainUnitModel])
	units: Sequence[T2IngestModel]
