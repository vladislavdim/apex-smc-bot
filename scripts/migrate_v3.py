"""Fail-closed V3 database migration entry point."""
from __future__ import annotations
import argparse,sqlite3
from apex.db.integrity import check_integrity

def main()->None:
    p=argparse.ArgumentParser(); p.add_argument("path"); a=p.parse_args()
    conn=sqlite3.connect(a.path)
    try:
        check_integrity(conn)
        # Schema migrations are owned by apex.db.state_db / apex.db.memory_db.
        # This command intentionally never invents or mutates schema implicitly.
        print("v3_db_integrity: OK")
    finally: conn.close()
if __name__=="__main__": main()
