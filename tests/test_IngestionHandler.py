import time
from collections import defaultdict
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Sequence, Set, Tuple, Union

import mongomock
import pytest
from pymongo import InsertOne, UpdateOne

from ampel.abstract.AbsStateT2Unit import AbsStateT2Unit
from ampel.abstract.AbsStockT2Unit import AbsStockT2Unit
from ampel.abstract.ingest.AbsAlertContentIngester import AbsAlertContentIngester
from ampel.abstract.ingest.AbsCompoundIngester import AbsCompoundIngester
from ampel.abstract.ingest.AbsStateT2Compiler import AbsStateT2Compiler
from ampel.abstract.ingest.AbsStateT2Ingester import AbsStateT2Ingester
from ampel.abstract.ingest.AbsT2Ingester import AbsT2Ingester
from ampel.alert.AmpelAlert import AmpelAlert
from ampel.alert.IngestionHandler import IngestionHandler
from ampel.content.Compound import Compound
from ampel.content.DataPoint import DataPoint
from ampel.content.StockRecord import StockRecord
from ampel.db.DBUpdatesBuffer import DBUpdatesBuffer
from ampel.demo.unit.base.DemoPointT2Unit import DemoPointT2Unit
from ampel.dev.DevAmpelContext import DevAmpelContext
from ampel.ingest.CompoundBluePrint import CompoundBluePrint
from ampel.ingest.PointT2Ingester import PointT2Ingester
from ampel.ingest.StockIngester import StockIngester
from ampel.ingest.StockT2Ingester import StockT2Ingester
from ampel.ingest.T1DefaultCombiner import T1DefaultCombiner
from ampel.log.AmpelLogger import AmpelLogger, DEBUG
from ampel.log.LogsBufferDict import LogsBufferDict
from ampel.model.AlertProcessorDirective import AlertProcessorDirective
from ampel.t2.T2RunState import T2RunState
from ampel.type import ChannelId, StockId, T2UnitResult


class DummyAlertContentIngester(AbsAlertContentIngester[AmpelAlert, DataPoint]):
    alert_history_length = 1

    def ingest(self, alert: AmpelAlert) -> List[DataPoint]:
        dps = [{"_id": i, "body": dp} for i, dp in enumerate(alert.dps)]
        for dp in dps:
            self.updates_buffer.add_t0_update(InsertOne(dp))
        return dps


class DummyCompoundIngester(AbsCompoundIngester):
    """simplified PhotoCompoundIngester for testing"""

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.channels: Set[ChannelId] = set()
        self.engine = T1DefaultCombiner(logger=AmpelLogger.get_logger(console=False))

    def add_channel(self, channel: ChannelId) -> None:
        self.channels.add(channel)

    def ingest(
        self,
        stock_id: StockId,
        datapoints: Sequence[DataPoint],
        chan_selection: List[Tuple[ChannelId, Union[bool, int]]],
    ) -> CompoundBluePrint:
        chans = [k for k, v in chan_selection if k in self.channels]

        blue_print = self.engine.combine(stock_id, datapoints, chans)

        for eff_comp_id in blue_print.get_effids_for_chans(chans):

            d_addtoset = {
                "channel": {
                    "$each": list(blue_print.get_chans_with_effid(eff_comp_id))
                },
                "run": self.run_id,
            }

            if blue_print.has_flavors(eff_comp_id):
                d_addtoset["flavor"] = {
                    "$each": blue_print.get_compound_flavors(eff_comp_id)
                }

            comp_dict = blue_print.get_eff_compound(eff_comp_id)

            comp_set_on_ins: PhotoCompound = {
                "_id": eff_comp_id,
                "stock": stock_id,
                "tag": list(blue_print.get_comp_tags(eff_comp_id)),
                "tier": 0,
                "added": time.time(),
                "len": len(comp_dict),
                "body": comp_dict,
            }

            self.updates_buffer.add_t1_update(
                UpdateOne(
                    {"_id": eff_comp_id},
                    {"$setOnInsert": comp_set_on_ins, "$addToSet": d_addtoset},
                    upsert=True,
                )
            )

        return blue_print


class DummyStateT2Compiler(AbsStateT2Compiler):
    def compile(
        self,
        chan_selection: List[Tuple[ChannelId, Union[bool, int]]],
        compound_blueprint: CompoundBluePrint,
    ) -> Dict[
        Tuple[str, Optional[int], Union[bytes, Tuple[bytes, ...]]], Set[ChannelId]
    ]:
        t2s_for_channels = defaultdict(set)
        for chan, ingest_model in self.get_ingest_models(chan_selection):
            t2s_for_channels[(ingest_model.unit_id, ingest_model.config)].add(chan)

        optimized_t2s = {}
        for k, v in t2s_for_channels.items():
            comp_ids = tuple(compound_blueprint.get_effids_for_chans(v))
            if len(comp_ids) == 1:
                optimized_t2s[k + comp_ids] = v
            else:
                optimized_t2s[k + (comp_ids,)] = v
        return optimized_t2s


class DummyStateT2Ingester(AbsStateT2Ingester):
    compiler: AbsStateT2Compiler[CompoundBluePrint] = DummyStateT2Compiler()

    def ingest(
        self,
        stock_id: StockId,
        comp_bp: CompoundBluePrint,
        chan_selection: List[Tuple[ChannelId, Union[bool, int]]],
    ) -> None:
        """
        Simplified version of PhotoT2Ingester, with t2 docs linked to exactly
        one compound
        """
        optimized_t2s = self.compiler.compile(chan_selection, comp_bp)
        now = int(time.time())

        # Loop over t2 units to be created
        for (t2_id, run_config, link_id), chans in optimized_t2s.items():

            # Matching search criteria
            match_dict: Dict[str, Any] = {
                "stock": stock_id,
                "unit": t2_id,
                "config": run_config
                # 'link' is added below
            }

            # Attributes set if no previous doc exists
            set_on_insert: T2Record = {
                "stock": stock_id,
                "tag": self.tags,
                "unit": t2_id,
                "config": run_config,
                "status": T2RunState.TO_RUN.value,
            }

            jchan, chan_add_to_set = AbsT2Ingester.build_query_parts(chans)
            add_to_set: Dict[str, Any] = {"channel": chan_add_to_set}

            assert isinstance(link_id, bytes)

            match_dict["link"] = {"$elemMatch": {"$eq": link_id}}
            add_to_set["link"] = link_id

            # Update journal
            add_to_set["journal"] = {"tier": self.tier, "dt": now, "channel": jchan}

            # Append update operation to bulk list
            self.updates_buffer.add_t2_update(
                UpdateOne(
                    match_dict,
                    {"$setOnInsert": set_on_insert, "$addToSet": add_to_set},
                    upsert=True,
                )
            )


class DummyStateT2Unit(AbsStateT2Unit):
    def run(self, compound: Compound, datapoints: Iterable[DataPoint]) -> T2UnitResult:
        return {"size": len(compound["body"])}


class DummyStockT2Unit(AbsStockT2Unit):
    def run(self, stock_record: StockRecord) -> T2UnitResult:
        return {"name": stock_record["name"]}


@pytest.fixture
def patch_mongo(monkeypatch):
    monkeypatch.setattr("ampel.db.AmpelDB.MongoClient", mongomock.MongoClient)


@pytest.fixture
def dev_context(patch_mongo):
    return DevAmpelContext.load(Path(__file__).parent / "testing-config.yaml",)


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
