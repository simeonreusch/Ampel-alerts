#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-alerts/ampel/model/AlertProcessorDirective.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.10.2019
# Last Modified Date: 10.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Optional, Dict, Literal
from ampel.model.UnitModel import UnitModel
from ampel.model.AutoStockMatchModel import AutoStockMatchModel
from ampel.model.ingest.IngestionDirective import IngestionDirective

class FilterModel(UnitModel):
	#: How to store rejection records
	reject: Optional[Dict[Literal['log', 'register'], UnitModel]]

class AlertProcessorDirective(IngestionDirective):
	#: How to select alerts 
	filter: Optional[FilterModel]
	#: How to handle new data after a stock passes the filter for the first time
	stock_match: Optional[AutoStockMatchModel]
