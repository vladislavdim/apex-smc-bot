from apex.config.constants import DASHBOARD_TABS,PRODUCTION_STRATEGIES
from apex.execution.protection import ProtectionState,ProtectionStatus
from apex.manager.state_machine import ManagerState,ManagerStatus
from apex.ui.dashboard.api import PROJECTORS

def test_production_strategy_set_is_exact():
    assert PRODUCTION_STRATEGIES==("FAST","MTF","ZONE","SWING","WYCKOFF")

def test_dashboard_tabs_are_exact():
    assert tuple(PROJECTORS)==DASHBOARD_TABS

def test_execution_owns_protection_state():
    assert ProtectionState.__module__=="apex.execution.protection"
    assert ProtectionStatus.__module__=="apex.execution.protection"

def test_manager_state_is_not_exchange_protection_state():
    assert ManagerState.__module__=="apex.manager.state_machine"
    assert ManagerStatus.__module__=="apex.manager.state_machine"
