
# Alert Lifecycle

# load
# deserialize

# Filter shape: morph into PhotoAlert

-> Instrument-specific implementation

Deserialized content is shaped into a format that the AlertProcessor understands.
A PhotoAlert typically contains two distinct flat sequences, one for photopoints and one for upperlimits. 
The tied object ID, such as the ZTF name, is converted into nummerical ampel IDs.
This is necessary for every alert since "autocomplete" is based on true Ampel IDs.

Implementation example: ZIAlertShaper
Note: the original alert content is kept/linked into the dict returned by ZIAlertShaper
because accepted alerts are later modified (which is not possible with ReadOnlyDicts)


# DB shape: morph into DataPoints

-> Instrument-specific implementation

Alerts that pass any T0 filter are further shaped in order to fullfill 
a few requirements for DB storage and easy later retrieval.
Among other things, individual datapoints can be tagged during this step.
For ZTF, upper limits do not feature a unique ID, so we have to build our own.
Each datapoint is shaped into the ampel.content.DataPoint structure.

Implementation example: ZIPhotoDataShaper
