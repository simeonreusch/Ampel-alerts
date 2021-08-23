import mongomock
import pytest
from pathlib import Path

from ampel.dev.DevAmpelContext import DevAmpelContext
from ampel.test.dummy import DummyPointT2Unit, DummyStateT2Unit, DummyStockT2Unit


@pytest.fixture
def patch_mongo(monkeypatch):
    monkeypatch.setattr("ampel.core.AmpelDB.MongoClient", mongomock.MongoClient)


@pytest.fixture
def testing_config():
    return Path(__file__).parent / "testing-config.yaml"


@pytest.fixture
def dev_context(patch_mongo, testing_config):
    return DevAmpelContext.load(testing_config)


@pytest.fixture
def dummy_units(dev_context: DevAmpelContext):

    for unit in (DummyStockT2Unit, DummyPointT2Unit, DummyStateT2Unit):
        dev_context.register_unit(unit)
