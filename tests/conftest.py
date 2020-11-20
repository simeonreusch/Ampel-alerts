from pathlib import Path

import mongomock
import pytest

from ampel.dev.DevAmpelContext import DevAmpelContext

@pytest.fixture
def patch_mongo(monkeypatch):
    monkeypatch.setattr("ampel.db.AmpelDB.MongoClient", mongomock.MongoClient)


@pytest.fixture
def dev_context(patch_mongo):
    return DevAmpelContext.load(Path(__file__).parent / "testing-config.yaml",)