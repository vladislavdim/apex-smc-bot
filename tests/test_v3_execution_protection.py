from apex.domain.enums import Direction
from apex.execution.protection import ProtectionRequest,valid_stop_replacement

def test_long_protection_only_tightens_below_market():
    assert valid_stop_replacement(ProtectionRequest(Direction.LONG,110,90,100))
    assert not valid_stop_replacement(ProtectionRequest(Direction.LONG,110,90,120))

def test_short_protection_only_tightens_above_market():
    assert valid_stop_replacement(ProtectionRequest(Direction.SHORT,90,110,100))
    assert not valid_stop_replacement(ProtectionRequest(Direction.SHORT,90,110,80))
