API reference
=============

Base classes
------------

.. autoclass:: ampel.abstract.AbsAlertFilter.AbsAlertFilter
  :members:

.. autoclass:: ampel.abstract.AbsAlertSupplier.AbsAlertSupplier
  :members:

Alert ingestion
---------------

.. autoclass:: ampel.alert.AlertProcessor.AlertProcessor
  :members:
  :exclude-members: sig_exit, set_cancel_run
  :show-inheritance:

.. autoclass:: ampel.model.AlertProcessorDirective.AlertProcessorDirective
  :members:
  :show-inheritance:

.. autoclass:: ampel.model.AlertProcessorDirective.FilterModel
  :members:
  :show-inheritance:

.. autoclass:: ampel.alert.FilterBlocksHandler.FilterBlocksHandler
  :members:

.. autoclass:: ampel.alert.IngestionHandler.IngestionHandler
  :members:

Alert loading
-------------

.. autoclass:: ampel.alert.load.TarAlertLoader.TarAlertLoader
  :members:

.. autoclass:: ampel.alert.load.FileAlertLoader.FileAlertLoader
  :members:

.. autoclass:: ampel.alert.load.DirAlertLoader.DirAlertLoader
  :members:

Rejected alerts handling
------------------------

Facilities for tracking rejected alerts

Database storage
****************

.. autoclass:: ampel.alert.reject.DBRejectedLogsHandler.DBRejectedLogsHandler
  :members:

File storage
************

In applications where most alerts are rejected, rejection records are likely to
be written much more often than they are read. In such cases, it can be much
more efficient to store (fixed-size) rejection records in an append-only log
than in a document-oriented database. :class:`~ampel.core.AmpelRegister.AmpelRegister`
implements such a log, and several subclasses exist to store rejection records
with various levels of detail. 

.. autoclass:: ampel.abstract.AbsAlertRegister.AbsAlertRegister
  :members:
  :show-inheritance:

.. autoclass:: ampel.alert.reject.BaseAlertRegister.BaseAlertRegister
  :members:
  :show-inheritance:

.. autoclass:: ampel.alert.reject.MinimalAlertRegister.MinimalAlertRegister
  :show-inheritance:

.. autoclass:: ampel.alert.reject.MinimalActiveAlertRegister.MinimalActiveAlertRegister
  :show-inheritance:

.. autoclass:: ampel.alert.reject.GeneralAlertRegister.GeneralAlertRegister
  :show-inheritance:

.. autoclass:: ampel.alert.reject.GeneralActiveAlertRegister.GeneralActiveAlertRegister
  :show-inheritance:

.. autoclass:: ampel.alert.reject.FullAlertRegister.FullAlertRegister
  :show-inheritance:

.. autoclass:: ampel.alert.reject.FullActiveAlertRegister.FullActiveAlertRegister
  :show-inheritance:
