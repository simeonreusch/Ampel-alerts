#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-alerts/ampel/model/AutoStockMatchModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 05.05.2020
# Last Modified Date: 05.05.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Literal
from ampel.model.StrictModel import StrictModel

class AutoStockMatchModel(StrictModel):
	filter: Literal['bypass', 'overrule']
	update_rej: bool = True
	retro_complete: bool
