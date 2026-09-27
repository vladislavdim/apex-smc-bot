from apex.config.constants import DASHBOARD_TABS,PRODUCTION_STRATEGIES
from apex.domain.enums import Direction
from apex.execution.protection import ProtectionRequest,valid_stop_replacement
from apex.ui.dashboard.api import PROJECTORS
from apex.ui.dashboard.auth import authorized


def test_production_strategy_contract_is_exact():
    assert PRODUCTION_STRATEGIES==("FAST","MTF","ZONE","SWING","WYCKOFF")


def test_dashboard_contract_has_only_v3_tabs():
    assert tuple(PROJECTORS)==DASHBOARD_TABS


def test_dashboard_auth_is_fail_closed():
    assert not authorized(None,"secret")
    assert not authorized("","secret")
    assert not authorized("wrong","secret")
    assert authorized("secret","secret")


def test_execution_protection_rejects_worse_long_stop():
    assert valid_stop_replacement(ProtectionRequest(Direction.LONG,110.0,100.0,105.0))
    assert not valid_stop_replacement(ProtectionRequest(Direction.LONG,110.0,100.0,95.0))
