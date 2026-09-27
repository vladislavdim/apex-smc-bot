"""Run canonical V3 State and Live Memory schema migrations only."""
from __future__ import annotations
import argparse
from apex.db.connection import connect_memory,connect_state
from apex.db.memory_db import migrate_memory_db
from apex.db.state_db import migrate_state_db

def main():
    p=argparse.ArgumentParser();p.add_argument("--state");p.add_argument("--memory");a=p.parse_args()
    s=connect_state(a.state) if a.state else connect_state();m=connect_memory(a.memory) if a.memory else connect_memory()
    try:migrate_state_db(s);migrate_memory_db(m)
    finally:s.close();m.close()
    print("v3_migrations: OK")
if __name__=="__main__":main()
