"""Initialize/upgrade canonical V3 State and Memory stores using repository migrations."""
from __future__ import annotations
import argparse
from apex.db.connection import connect_memory,connect_state

def main():
    p=argparse.ArgumentParser(); p.add_argument("--state"); p.add_argument("--memory"); a=p.parse_args()
    state=connect_state(a.state) if a.state else connect_state()
    memory=connect_memory(a.memory) if a.memory else connect_memory()
    try:
        print("state_db: READY"); print("memory_db: READY")
    finally:
        state.close(); memory.close()
if __name__=="__main__": main()
