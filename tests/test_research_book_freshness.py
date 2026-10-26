"""Keep REST book freshness relative to decision, not receipt, time."""
from datetime import datetime, timezone
from types import SimpleNamespace as NS
import pytest
from apex.market.live_context import fetch_live_context
from apex.market.provider import GateSnapshotProvider
from apex.market.candles import TIMEFRAME_SECONDS

AS_OF=1792800000


class Client:
    def __init__(self,age):self.age=age
    def order_book(self,*a,**kw):return NS(payload={'id':42,'bids':[[99,10]],'asks':[[101,1]]},received_at=AS_OF-self.age)
    def contract_multiplier(self,*a,**kw):return 1.
    def contract_stats(self,*a,**kw):return NS(payload=[],received_at=AS_OF)
    def funding_history(self,*a,**kw):return NS(payload=[],received_at=AS_OF)
    def public_trades(self,*a,**kw):return NS(payload=[],received_at=AS_OF)
    def candles(self,symbol,timeframe,limit):
        step=TIMEFRAME_SECONDS[timeframe];end=(AS_OF//step)*step
        return NS(received_at=AS_OF,payload=[dict(open_time=end-step*(100-i),open=100,high=102,low=98,close=100,volume=100) for i in range(100)])


@pytest.mark.parametrize('age',[0,2,5,6,20,-1])
def test_book_uses_decision_clock(age):
    book=fetch_live_context(Client(age),'BTCUSDT',as_of=AS_OF).microstructure['visible_orderbook']
    assert book['available']==(0<=age<=5)
    if age>=0:assert book['age_seconds']==age


@pytest.mark.parametrize('age',[2,20])
def test_provider_exports_only_fresh_book(age):
    result=GateSnapshotProvider(Client(age)).build('MTF','ETHUSDT',as_of=datetime.fromtimestamp(AS_OF,timezone.utc))
    assert ('strategy_orderbook' in result.snapshot.microstructure_context)==(age<=5)
