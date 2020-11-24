from ampel.alert.AlertProcessor import AlertProcessor
from ampel.alert.AmpelAlert import AmpelAlert
from ampel.dev.UnitTestAlertSupplier import UnitTestAlertSupplier


def test_no_filter(dev_context, legacy_directive):
    ap = AlertProcessor(
        context=dev_context,
        process_name="ap",
        publish_stats=["mongo"],
        directives=[legacy_directive],
        supplier=UnitTestAlertSupplier(
            alerts=[AmpelAlert(id="alert", stock_id="stockystock", dps=[])]
        ),
    )
    assert ap.run() == 1
    assert dev_context.db.get_collection("stock").count_documents({}) == 1
    assert "metrics" in (event := dev_context.db.get_collection("events").find_one())
