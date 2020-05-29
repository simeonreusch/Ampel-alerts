#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-alerts/ampel/model/T2IngestModel.py
# License           : BSD-3-Clause
# Author            : vb <vbrinnel@physik.hu-berlin.de>
# Date              : 10.03.2020
# Last Modified Date: 11.05.2020
# Last Modified By  : vb <vbrinnel@physik.hu-berlin.de>

from typing import Any, Dict, List, Union, Optional
from ampel.model.AmpelStrictModel import AmpelStrictModel
from ampel.util.mappings import build_unsafe_short_dict_id

class T2IngestModel(AmpelStrictModel):

	unit: str
	config: int = build_unsafe_short_dict_id(None)
	ingest: Optional[Dict[str, Any]]
	group: Union[int, List[int]] = []
