"""Apply canonical APEX V3 State and Live Memory migrations explicitly."""
from __future__ import annotations
import argparse,sqlite3
from apex.db.integrity import check_integrity
from apex.db.state_db import migrate_state
from apex.db.memory_db import migrate_memory

def _migrate(path,fn):
    conn=sqlite3.connect(path)
    try:
        applied=fn(conn); check_integrity(conn); return applied
    finally: conn.close()

def main():
    p=argparse.ArgumentParser(); p.add_argument("--state",required=True); p.add_argument("--memory",required=True); a=p.parse_args()
    print("state",_migrate(a.state,migrate_state)); print("memory",_migrate(a.memory,migrate_memory))
if __name__=="__main__": main()
