"""Learning Telegram presentation boundary.
Learning is advisory and has no execution authority.
"""
def learning_line(status:str|None,samples:int=0)->str:
    return f"Learning {status or 'UNKNOWN'} · real outcomes {int(samples)}"
__all__=["learning_line"]
