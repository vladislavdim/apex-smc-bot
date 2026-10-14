import pytest
from apex.ui.readmodels.api import PROJECTORS,project_tab
from apex.config.constants import UI_SECTIONS,PRODUCTION_STRATEGIES

def test_unknown_tab_rejected():
    with pytest.raises(ValueError): project_tab("research",{})
def test_only_five_production_strategies(): assert PRODUCTION_STRATEGIES==("FAST","MTF","ZONE","SWING","WYCKOFF")
