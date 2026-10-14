import pytest
from apex.config.constants import UI_SECTIONS,PRODUCTION_STRATEGIES

def test_only_five_production_strategies(): assert PRODUCTION_STRATEGIES==("FAST","MTF","ZONE","SWING","WYCKOFF")
