#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-alerts/ampel/model/T2IngestModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 10.03.2020
# Last Modified Date: 03.06.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Any, Dict, List, Union, Optional
from ampel.model.AmpelStrictModel import AmpelStrictModel

class T2IngestModel(AmpelStrictModel):

	unit: str
	config: Optional[int]
	ingest: Optional[Dict[str, Any]]
	group: Union[int, List[int]] = []
