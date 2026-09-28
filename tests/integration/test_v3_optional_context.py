from apex.market.external_context import external_context
from apex.market.onchain import onchain_context
from apex.market.options import options_context

def test_optional_context_unknown_is_not_zero():
    assert external_context({},fresh=False,source="x")["state"]=="UNKNOWN"
    assert options_context()["state"]=="UNKNOWN"
    assert onchain_context()["state"]=="UNKNOWN"

def test_optional_context_keeps_provenance():
    ctx=external_context({"oi":12},fresh=True,source="coinalyze")
    assert ctx["state"]=="REAL" and ctx["source"]=="coinalyze"
