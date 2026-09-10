import tempfile
import unittest
from pathlib import Path

from research.market_context import (
    GATE_DERIVATIVES_HISTORY_SECONDS, GateMarketContextClient,
    PointInTimeContextIndex, collect_market_context,
)
from research.store import ResearchStore
from research.replay import _attempt_checks


class FakeHistoryClient:
    def __init__(self):
        self.calls=[]

    def _get(self,url,params=None):
        self.calls.append((url,dict(params or {})))
        if url.endswith("/funding_rate"):
            return [{"t":100,"r":"0.0002"},{"t":200,"r":"-0.0001"}]
        if url.endswith("/contract_stats"):
            return [{"time":100,"open_interest":1000,"open_interest_usd":50000,
                "lsr_account":1.1,"lsr_taker":.9,"top_lsr_account":1.2,"top_lsr_size":1.3,
                "long_liq_size":2,"short_liq_size":3,"long_liq_usd":200,"short_liq_usd":300},
                {"time":200,"open_interest":1100,"open_interest_usd":55000,
                "lsr_account":1.25,"lsr_taker":1.1,"long_liq_usd":500,"short_liq_usd":100}]
        if url.endswith("/trades"):
            return [{"id":1,"create_time":150,"size":2,"price":"10"},
                    {"id":2,"create_time":151,"size":-1,"price":"10"},
                    {"id":3,"create_time":152,"size":99,"price":"1","is_internal":True}]
        if url.endswith("/order_book"):
            return {"id":9,"current":200,"bids":[["9.9","10"]],"asks":[["10.1","5"]]}
        raise AssertionError(url)


class ResearchMarketContextTests(unittest.TestCase):
    def test_collects_six_shadow_features_without_execution_authority(self):
        with tempfile.TemporaryDirectory() as tmp:
            store=ResearchStore(str(Path(tmp)/"research.db")); store.ensure_schema()
            client=FakeHistoryClient()
            result=collect_market_context(store,client,"BTCUSDT",0,300)
            self.assertEqual(set(result["features"]),{
                "TRADE_CVD_REAL","OPEN_INTEREST","FUNDING_RATE","LIQUIDATIONS",
                "ORDER_BOOK_LIQUIDITY","LONG_SHORT_RATIO"})
            rows=store.context_rows("BTCUSDT")
            self.assertTrue(rows)
            self.assertTrue(all(row["point_in_time"] for row in rows))
            self.assertNotIn("execution",str(rows).lower())
            cvd=next(row for row in rows if row["feature"]=="TRADE_CVD_REAL")
            self.assertEqual(cvd["value"]["delta_notional"],10)
            book=next(row for row in rows if row["feature"]=="ORDER_BOOK_LIQUIDITY")
            self.assertEqual(book["availability"],"FORWARD_ONLY")
            self.assertEqual(len(book["value"]["heatmap_levels"]),2)
            dashboard=store.dashboard()
            self.assertEqual(len({x["feature"] for x in dashboard["market_context"]}),6)

    def test_point_in_time_index_never_uses_future_observation(self):
        index=PointInTimeContextIndex([
            {"feature":"FUNDING_RATE","event_time":100,"source":"GATE_FUNDING",
             "quality":"VALID","availability":"HISTORICAL","value":{"rate":.01}},
            {"feature":"FUNDING_RATE","event_time":200,"source":"GATE_FUNDING",
             "quality":"VALID","availability":"HISTORICAL","value":{"rate":.02}},
        ])
        self.assertEqual(index.as_of(150)["funding_rate"]["rate"],.01)
        self.assertEqual(index.as_of(200)["funding_rate"]["rate"],.02)
        self.assertEqual(index.as_of(99),{})

    def test_stale_forward_context_is_not_joined_to_later_candles(self):
        index=PointInTimeContextIndex([{"feature":"ORDER_BOOK_LIQUIDITY",
            "event_time":100,"source":"GATE_ORDER_BOOK","quality":"VALID",
            "availability":"FORWARD_ONLY","value":{"depth_imbalance":.5}}])
        self.assertIn("order_book_liquidity",index.as_of(110))
        self.assertNotIn("order_book_liquidity",index.as_of(116))

    def test_gate_requests_are_public_read_only(self):
        fake=FakeHistoryClient(); client=GateMarketContextClient(fake)
        client.funding("BTCUSDT",0,300); client.contract_stats("BTCUSDT",0,300)
        client.recent_trade_cvd("BTCUSDT",0,300); client.order_book("BTCUSDT")
        self.assertTrue(fake.calls)
        self.assertTrue(all("/futures/usdt/" in url for url,_ in fake.calls))
        self.assertEqual({url.rsplit("/",1)[-1] for url,_ in fake.calls},
                         {"funding_rate","contract_stats","trades","order_book"})

    def test_derivatives_history_is_clamped_to_gate_retention(self):
        fake=FakeHistoryClient(); client=GateMarketContextClient(fake)
        end=GATE_DERIVATIVES_HISTORY_SECONDS+10_000
        client.funding("BTCUSDT",0,end); client.contract_stats("BTCUSDT",0,end)
        starts=[params["from"] for url,params in fake.calls
                if url.endswith(("/funding_rate","/contract_stats"))]
        self.assertTrue(starts)
        self.assertTrue(all(start == end-GATE_DERIVATIVES_HISTORY_SECONDS+1 for start in starts))

    def test_all_new_inputs_are_auxiliary_and_cannot_change_gate_result(self):
        snapshots={"15m":{"as_of":200,"data_quality":{"status":"INVALID"},
            "derivatives":{"funding_rate":{"rate":.1},"open_interest":{"usd":1},
                "trade_cvd_real":{"delta_notional":1},"liquidations":{"long_usd":1},
                "order_book_liquidity":{"depth_imbalance":1},
                "long_short_ratio":{"accounts":2}}}}
        checks=_attempt_checks("FAST",snapshots,{"technical_evidence":{}},"DATA_QUALITY")
        shadow=[x for x in checks if x["role"]=="SHADOW_CONTEXT"]
        self.assertEqual(len(shadow),6)
        self.assertTrue(all(x["status"]=="OBSERVED" for x in shadow))
        self.assertTrue(all(x["threshold"]["value"] is None for x in shadow))
        self.assertTrue(all(x["evidence"]["execution_authority"] is False for x in shadow))
        self.assertEqual(next(x for x in checks if x["check_code"]=="DATA_QUALITY")["status"],"FAIL")


if __name__ == "__main__":
    unittest.main()
