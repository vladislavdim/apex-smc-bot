from apex.domain.enums import Direction
from apex.manager.eligibility import ManagerFacts
from apex.manager.playbooks import eligible_actions,enforce_action

def test_manager_cannot_invent_ineligible_action():
    facts=ManagerFacts(direction=Direction.LONG,current_price=100,confirmed_stop=90,remaining_quantity=1)
    assert eligible_actions(facts)==("HOLD",)
    assert enforce_action("CLOSE",facts)=="HOLD"
