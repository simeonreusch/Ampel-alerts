## Alert management for AMPEL
The central module of this repository is `ampel.alert.AlertProcessor`


## Processing of alerts through AMPEL

### Load (tar, network, ...)
### Deserialize (avro, bson, json, ...)

- The later steps are instrument specific

### First shape: morph into `AmpelAlert` or `PhotoAlert`

Purpose: having a common format that the `AlertProcessor` and alert filters understand.
A `PhotoAlert` typically contains two distinct flat sequences, one for photopoints and one for upperlimits.
The associated object ID, such as the ZTF name, is converted into nummerical ampel IDs.
This is necessary for all alerts (rejected one as well) since "autocomplete" is based on true Ampel IDs.

Implementation example: `ampel.ztf.alert.ZIAlertSupplier`


### Second shape: morph into `DataPoint`

Alerts that pass any T0 filter are further shaped in order to fullfill
a few requirements for DB storage and easy later retrieval.
Among other things, individual datapoints can be tagged during this step.
For ZTF, upper limits do not feature a unique ID, so we have to build our own.
Each datapoint is shaped into a `ampel.content.DataPoint` structure.

Implementation example: `ampel.ztf.ingest.ZiT0PhotoPointShaper`
