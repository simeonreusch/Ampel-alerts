#!/usr/bin/env python
# -*- coding: utf-8 -*-
# File              : Ampel-alerts/tests/test_AlertProcessor.py
# License           : BSD-3-Clause
# Author            : jvs
# Date              : Unspecified
# Last Modified Date: 05.08.2021
# Last Modified By  : vb

import os, signal, time, threading
from contextlib import contextmanager

from ampel.alert.AlertConsumer import AlertConsumer
from ampel.alert.AlertConsumerError import AlertConsumerError
from ampel.alert.AmpelAlert import AmpelAlert
from ampel.alert.filter.BasicMultiFilter import BasicMultiFilter
from ampel.dev.UnitTestAlertSupplier import UnitTestAlertSupplier
from ampel.metrics.AmpelMetricsRegistry import AmpelMetricsRegistry
from ampel.model.ingest.FilterModel import FilterModel


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
        ap = AlertConsumer(
            context=dev_context,
            process_name="ap",
            shaper = "NoShaper",
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
        ap = AlertConsumer(
            context=dev_context,
            process_name="ap",
            shaper = "NoShaper",
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
    assert stats[("ampel_alertprocessor_time_seconds_sum", (("section", "ingest"),),)] > 0
    assert (
        stats[
            (
                "ampel_alertprocessor_time_seconds_sum",
                (("section", "filter.TEST_CHANNEL"),),
            )
        ]
        > 0
    )


def test_suspend_in_supplier(dev_context, legacy_directive=None):

    # simulate a producer that blocks while waiting fo upstream input
    class BlockingAlertSupplier(UnitTestAlertSupplier):
        def __iter__(self):
            for el in super().__iter__():
                print("sleep 3")
                time.sleep(3)
                yield el

    dev_context.register_unit(BlockingAlertSupplier)
    ap = AlertConsumer(
        context = dev_context,
        process_name = "ap",
        shaper = "NoShaper",
        directives = [legacy_directive or {"channel": "CHAN1", "filter": None}],
        supplier = {
            "unit": "BlockingAlertSupplier",
            "config": {
                "alerts": [AmpelAlert(id="alert", stock_id="stockystock", dps=[])]
            }
        }
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
        BasicMultiFilter, "process", lambda *args: os.kill(os.getpid(), signal.SIGINT)
    )
    legacy_directive.filter = FilterModel(unit=BasicMultiFilter, config={"filters": []})
    ap = AlertConsumer(
        context=dev_context,
        process_name="ap",
        directives=[legacy_directive],
        supplier=UnitTestAlertSupplier(
            alerts=[AmpelAlert(id="alert", stock_id="stockystock", dps=[])]
        ),
    )
    assert ap.run() == 1, "AP successfully processes alert"
    assert ap._cancel_run == AlertConsumerError.SIGINT
