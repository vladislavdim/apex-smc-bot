"""Execution-owned exchange protection primitives.
Manager decides eligibility; Execution validates and applies stop geometry.
"""
from __future__ import annotations
from apex.domain.enums import Direction
def stop_is_protective(direction:Direction,current_stop:float,new_stop:float,current_price:float)->bool:
    if direction is Direction.LONG: return current_stop < new_stop < current_price
    return current_price < new_stop < current_stop
def require_protective_stop(direction:Direction,current_stop:float,new_stop:float,current_price:float)->float:
    value=float(new_stop)
    if not stop_is_protective(direction,float(current_stop),value,float(current_price)):
        raise ValueError("NON_PROTECTIVE_STOP_FORBIDDEN")
    return value
__all__=["require_protective_stop","stop_is_protective"]
