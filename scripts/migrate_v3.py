"""Integrity-first V3 database migration entry point.
Schema ownership remains in apex.db; this script only invokes canonical migration hooks.
"""
from __future__ import annotations
import argparse,sqlite3
from apex.db.integrity import check_integrity

def main():
    p=argparse.ArgumentParser(); p.add_argument("path"); a=p.parse_args()
    conn=sqlite3.connect(a.path)
    try:
        check_integrity(conn)
        print("pre_migration_integrity: OK")
    finally: conn.close()
if __name__=="__main__": main()
