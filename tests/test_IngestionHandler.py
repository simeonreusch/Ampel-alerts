import pytest

from ampel.alert.AmpelAlert import AmpelAlert
from ampel.alert.IngestionHandler import IngestionHandler
from ampel.db.DBUpdatesBuffer import DBUpdatesBuffer
from ampel.demo.unit.base.DemoPointT2Unit import DemoPointT2Unit
from ampel.ingest.PointT2Ingester import PointT2Ingester
from ampel.ingest.StockIngester import StockIngester
from ampel.ingest.StockT2Ingester import StockT2Ingester
from ampel.log.AmpelLogger import AmpelLogger, DEBUG
from ampel.log.LogsBufferDict import LogsBufferDict
from ampel.model.AlertProcessorDirective import AlertProcessorDirective

from .dummy_units import (
    DummyAlertContentIngester,
    DummyCompoundIngester,
    DummyStateT2Ingester,
    DummyStateT2Unit,
    DummyStockT2Unit,
)


def get_handler(context, directives):
    run_id = 0
    logger = AmpelLogger.get_logger(console={"level": DEBUG})
    updates_buffer = DBUpdatesBuffer(context.db, run_id=run_id, logger=logger)
    logd = LogsBufferDict({"logs": [], "extra": {}})
    return IngestionHandler(
        context=context,
        logger=logger,
        run_id=0,
        updates_buffer=updates_buffer,
        directives=directives,
    )


def test_no_directive(dev_context):
    with pytest.raises(ValueError):
        get_handler(dev_context, [])


def test_minimal_directive(dev_context):
    directive = {
        "channel": "TEST_CHANNEL",
        "stock_update": {"unit": StockIngester},
    }
    handler = get_handler(dev_context, [AlertProcessorDirective(**directive)])
    assert isinstance(handler.stock_ingester, StockIngester)
    handler.logd["logs"].append(f"doing a good thing")
    handler.ingest(
        AmpelAlert(id="alert", stock_id="stockystock", dps=[]), [("TEST_CHANNEL", True)]
    )
    handler.logd["logs"].append(f"doing a bad thing")
    handler.logd["err"] = True
    handler.ingest(
        AmpelAlert(id="alert", stock_id="stockystock", dps=[]), [("TEST_CHANNEL", True)]
    )
    handler.updates_buffer.push_updates()
    stock = dev_context.db.get_collection("stock")
    assert stock.count_documents({}) == 1
    assert stock.count_documents({"_id": "stockystock"}) == 1


def test_legacy_directive(dev_context):
    directive = {
        "channel": "TEST_CHANNEL",
        "stock_update": {"unit": StockIngester},
        "t0_add": {
            "ingester": DummyAlertContentIngester,
            "t1_combine": [
                {
                    "ingester": DummyCompoundIngester,
                    "t2_compute": {
                        "ingester": DummyStateT2Ingester,
                        "units": [{"unit": DummyStateT2Unit}],
                    },
                }
            ],
            "t2_compute": {
                "ingester": PointT2Ingester,
                "units": [{"unit": DemoPointT2Unit}],
            },
        },
        "t2_compute": {
            "ingester": StockT2Ingester,
            "units": [{"unit": DummyStockT2Unit}],
        },
    }
    handler = get_handler(dev_context, [AlertProcessorDirective(**directive)])
    assert isinstance(handler.stock_ingester, StockIngester)
    assert isinstance(handler.datapoint_ingester, DummyAlertContentIngester)

    handler.retro_complete = ["TEST_CHANNEL"]
    handler.ingest(
        AmpelAlert(id="alert", stock_id="stockystock", dps=[{}, {}]),
        [("TEST_CHANNEL", True)],
    )
    handler.updates_buffer.push_updates()
    t0 = dev_context.db.get_collection("t0")
    assert t0.count_documents({}) == 2
    assert t0.count_documents({"_id": 0}) == 1

    t1 = dev_context.db.get_collection("t1")
    assert t1.count_documents({}) == 2

    t2 = dev_context.db.get_collection("t2")
    assert len(docs := list(t2.find({}))) == 2 + 3
    print(docs)
    assert docs[0]["stock"] == "stockystock"
    assert docs[0]["unit"] == "DummyStockT2Unit"
    assert "link" not in docs[0]
    for i in range(1, 3):
        assert docs[i]["stock"] == "stockystock"
        assert docs[i]["unit"] == "DemoPointT2Unit"
        assert isinstance(docs[i]["link"], int)
    for i in range(3, 5):
        assert docs[i]["stock"] == "stockystock"
        assert docs[i]["unit"] == "DummyStateT2Unit"
        assert len(docs[i]["link"]) == 1


def test_standalone_t1_directive(dev_context):
    directive = {
        "channel": "TEST_CHANNEL",
        "stock_update": {"unit": StockIngester},
        "t0_add": {
            "ingester": DummyAlertContentIngester,
            "t1_combine": [
                {
                    "ingester": DummyCompoundIngester,
                    "t2_compute": {
                        "ingester": DummyStateT2Ingester,
                        "units": [{"unit": DummyStateT2Unit}],
                    },
                }
            ],
        },
        "t1_combine": [
            {
                "ingester": DummyCompoundIngester,
                "t2_compute": {
                    "ingester": DummyStateT2Ingester,
                    "units": [{"unit": DummyStateT2Unit}],
                },
            }
        ],
    }
    with pytest.raises(NotImplementedError):
        handler = get_handler(dev_context, [AlertProcessorDirective(**directive)])
