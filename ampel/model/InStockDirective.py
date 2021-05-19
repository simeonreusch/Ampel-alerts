#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-alerts/ampel/model/InStockDirective.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 05.05.2020
# Last Modified Date: 19.05.2021
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Literal
from ampel.model.StrictModel import StrictModel

class InStockDirective(StrictModel):
	filter_result: Literal['bypass', 'overrule']
	update_rej: bool = True
	retro_combine: bool = False
