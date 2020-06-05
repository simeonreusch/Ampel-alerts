#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-alerts/ampel/model/AlertProcessorDirective.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 14.10.2019
# Last Modified Date: 05.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Union, Optional, Dict, Literal
from ampel.model.DataUnitModel import DataUnitModel
from ampel.model.AliasedDataUnitModel import AliasedDataUnitModel
from ampel.model.AutoStockMatchModel import AutoStockMatchModel
from ampel.model.ingest.IngestionDirective import IngestionDirective

class AliasedFilterModel(AliasedDataUnitModel):
	reject: Optional[Dict[Literal['log', 'register'], Union[AliasedDataUnitModel, DataUnitModel]]]

class FilterModel(DataUnitModel):
	reject: Optional[Dict[Literal['log', 'register'], Union[AliasedDataUnitModel, DataUnitModel]]]

class AlertProcessorDirective(IngestionDirective):
	filter: Optional[Union[FilterModel, AliasedFilterModel]]
	stock_match: Optional[AutoStockMatchModel]
