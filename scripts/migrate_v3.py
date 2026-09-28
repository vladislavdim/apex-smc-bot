"""Run canonical V3 State/Memory schema migrations only.
Compatibility databases are intentionally outside this migration entry point.
"""
from __future__ import annotations
from apex.db.connection import connect_state,connect_memory
from apex.db.state_db import migrate_state_db
from apex.db.memory_db import migrate_memory_db

def main()->None:
    state=connect_state(); memory=connect_memory()
    try:
        migrate_state_db(state); migrate_memory_db(memory)
        print("v3_migrations: OK")
    finally:
        state.close(); memory.close()
if __name__=="__main__": main()
