from contextlib import contextmanager

from ampel.alert.AlertProcessor import AlertProcessor
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
    assert stats[("ampel_alertprocessor_time_seconds_sum", (("section", "main"),),)] > 0
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
    assert stats[("ampel_alertprocessor_time_seconds_sum", (("section", "main"),),)] > 0
    assert (
        stats[
            (
                "ampel_alertprocessor_time_seconds_sum",
                (("section", "filter.TEST_CHANNEL"),),
            )
        ]
        > 0
    )
