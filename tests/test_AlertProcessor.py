import os
import signal
import time
import threading
from contextlib import contextmanager
from unittest.mock import MagicMock

from ampel.core.AmpelContext import AmpelContext

from ampel.alert.AlertProcessor import AlertProcessor, INTERRUPTED
from ampel.alert.AmpelAlert import AmpelAlert
from ampel.alert.filter.BasicMultiFilter import BasicMultiFilter
from ampel.dev.UnitTestAlertSupplier import UnitTestAlertSupplier
from ampel.metrics.AmpelMetricsRegistry import AmpelMetricsRegistry
from ampel.model.AlertProcessorDirective import FilterModel


@contextmanager
def collect_diff(store):
    store.clear()
    before = {}
    for metric in AmpelMetricsRegistry.registry().collect():
        for sample in metric.samples:
            key = (sample.name, tuple(sample.labels.items()))
            before[key] = sample.value

    delta = {}
    yield
    for metric in AmpelMetricsRegistry.registry().collect():
        for sample in metric.samples:
            key = (sample.name, tuple(sample.labels.items()))
            delta[key] = sample.value - (before.get(key) or 0)
    store.update(delta)


def test_no_filter(dev_context, legacy_directive):
    stats = {}
    with collect_diff(stats):
        ap = AlertProcessor(
            context=dev_context,
            process_name="ap",
            directives=[legacy_directive],
            supplier=UnitTestAlertSupplier(
                alerts=[AmpelAlert(id="alert", stock_id="stockystock", dps=[])]
            ),
        )
        assert ap.run() == 1

    assert dev_context.db.get_collection("stock").count_documents({}) == 1
    assert stats[("ampel_alertprocessor_alerts_processed_total", ())] == 1
    assert (
        stats[
            (
                "ampel_alertprocessor_alerts_accepted_total",
                (("channel", "TEST_CHANNEL"),),
            )
        ]
        == 1
    )
    assert (
        stats[
            (
                "ampel_alertprocessor_time_seconds_sum",
                (("section", "ingest"),),
            )
        ]
        > 0
    )
    assert (
        stats[
            (
                "ampel_alertprocessor_time_seconds_sum",
                (("section", "filter.TEST_CHANNEL"),),
            )
        ]
        == 0
    )


def test_with_filter(dev_context, legacy_directive):
    stats = {}
    with collect_diff(stats):
        legacy_directive.filter = FilterModel(
            unit=BasicMultiFilter,
            config={
                "filters": [
                    {
                        "criteria": [
                            {"attribute": "nonesuch", "value": 0, "operator": "=="}
                        ],
                        "len": 0,
                        "operator": "==",
                    }
                ]
            },
        )
        ap = AlertProcessor(
            context=dev_context,
            process_name="ap",
            directives=[legacy_directive],
            supplier=UnitTestAlertSupplier(
                alerts=[AmpelAlert(id="alert", stock_id="stockystock", dps=[])]
            ),
        )
        assert ap.run() == 1

    assert dev_context.db.get_collection("stock").count_documents({}) == 1
    assert stats[("ampel_alertprocessor_alerts_processed_total", ())] == 1
    assert (
        stats[
            (
                "ampel_alertprocessor_alerts_accepted_total",
                (("channel", "TEST_CHANNEL"),),
            )
        ]
        == 1
    )
    assert (
        stats[
            (
                "ampel_alertprocessor_time_seconds_sum",
                (("section", "ingest"),),
            )
        ]
        > 0
    )
    assert (
        stats[
            (
                "ampel_alertprocessor_time_seconds_sum",
                (("section", "filter.TEST_CHANNEL"),),
            )
        ]
        > 0
    )


def test_suspend_in_supplier(dev_context, legacy_directive):
    # simulate a producer that blocks while waiting fo upstream input
    class BlockingAlertSupplier(UnitTestAlertSupplier):
        def __next__(self):
            time.sleep(3)
            return super().__next__()

    ap = AlertProcessor(
        context=dev_context,
        process_name="ap",
        directives=[legacy_directive],
        supplier=BlockingAlertSupplier(
            alerts=[AmpelAlert(id="alert", stock_id="stockystock", dps=[])]
        ),
    )

    def alarm():
        time.sleep(0.5)
        os.kill(os.getpid(), signal.SIGINT)

    t = threading.Thread(target=alarm)
    t.start()
    t0 = time.time()
    assert ap.run() == 0, "AP suspended before first alert was processed"
    assert time.time() - t0 < 2, "AP suspended before supplier timed out"
    t.join()


def test_suspend_in_critical_section(dev_context, legacy_directive, monkeypatch):
    monkeypatch.setattr(
        BasicMultiFilter, "apply", lambda *args: os.kill(os.getpid(), signal.SIGINT)
    )
    legacy_directive.filter = FilterModel(unit=BasicMultiFilter, config={"filters": []})
    ap = AlertProcessor(
        context=dev_context,
        process_name="ap",
        directives=[legacy_directive],
        supplier=UnitTestAlertSupplier(
            alerts=[AmpelAlert(id="alert", stock_id="stockystock", dps=[])]
        ),
    )
    assert ap.run() == 1, "AP successfully processes alert"
    assert ap._cancel_run == INTERRUPTED


def test_error_reporting(dev_context: AmpelContext, legacy_directive, monkeypatch):
    """
    channel is set in troubles doc
    """
    monkeypatch.setattr(
        BasicMultiFilter, "apply", MagicMock(side_effect=KeyError("baaaad"))
    )
    legacy_directive.filter = FilterModel(unit=BasicMultiFilter, config={"filters": []})
    ap = AlertProcessor(
        context=dev_context,
        process_name="ap",
        directives=[legacy_directive],
        supplier=UnitTestAlertSupplier(
            alerts=[AmpelAlert(id="alert", stock_id="stockystock", dps=[])]
        ),
    )
    assert ap.run() == 1
    assert (doc := dev_context.db.get_collection("troubles").find_one({}))
    assert doc["channel"] == "TEST_CHANNEL"
    assert "KeyError: 'baaaad" in "\n".join(doc["exception"])
