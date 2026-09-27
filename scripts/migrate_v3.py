"""Fail-explicit V3 migration entrypoint.
Schema migrations remain owned by canonical State/Memory DB modules; this command
only invokes their public migration surfaces and never touches Binance.
"""
from __future__ import annotations
import argparse
from apex.db.connection import connect_memory,connect_state
from apex.db.memory_db import migrate_memory_db
from apex.db.state_db import migrate_state_db
def main():
    p=argparse.ArgumentParser();p.add_argument("--state",action="store_true");p.add_argument("--memory",action="store_true");a=p.parse_args()
    if not (a.state or a.memory): raise SystemExit("select --state and/or --memory")
    if a.state:
        c=connect_state();
        try:migrate_state_db(c)
        finally:c.close()
    if a.memory:
        c=connect_memory();
        try:migrate_memory_db(c)
        finally:c.close()
if __name__=="__main__":main()
