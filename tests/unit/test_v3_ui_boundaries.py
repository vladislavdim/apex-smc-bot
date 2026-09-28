import pytest
from apex.ui.dashboard.api import PROJECTORS,project_tab
from apex.ui.dashboard.auth import authorized
from apex.config.constants import DASHBOARD_TABS,PRODUCTION_STRATEGIES

def test_dashboard_has_exact_production_tabs(): assert tuple(PROJECTORS)==DASHBOARD_TABS
def test_dashboard_auth_is_fail_closed():
    assert authorized("same","same");assert not authorized(None,"same");assert not authorized("x","")
def test_unknown_tab_rejected():
    with pytest.raises(ValueError): project_tab("research",{})
def test_only_five_production_strategies(): assert PRODUCTION_STRATEGIES==("FAST","MTF","ZONE","SWING","WYCKOFF")
